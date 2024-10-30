import discord
from discord import app_commands
import asyncio
import random
import string
import os
import shutil
import aiohttp
import json
import logging
import time
from collections import deque
import aiofiles
from aiohttp import ClientTimeout
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def start_archival(url, q_num, server_id):
    """Starts the archival process using ytarchive."""
    dir_path = f'q_{server_id}_{q_num}'
    temp_dir = f'temp_q_{server_id}_{q_num}'
    command = [
        "ytarchive", "-w", "-l", "-t", "-o", f"{dir_path}/[%(upload_date)s] %(title)s", "--vp9",
        "--no-save", "--no-save-state",
        "--write-description", "--write-thumbnail", "--retry-frags", "10",
        "--no-frag-files", "--add-metadata", "-td", temp_dir,
        url, "best"
    ]
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    archiver_stdout, _ = await process.communicate()
    os.makedirs(dir_path, exist_ok=True)
    open(f'{dir_path}/origin.txt', 'x').close()  # generate the file to store the pre-sanitized file names
    return q_num, archiver_stdout.decode()

def generate_id(n=16):
    """Generates a random alphanumeric ID of length `n`."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

def sanitize_filename(filename):
    """Maps file extensions to type labels for the origin file."""
    if filename.endswith(('.mp4', '.mkv')):
        return 'video'
    elif filename.endswith('.jpg'):
        return 'img'
    elif filename.endswith('.description'):
        return 'desc'
    return filename

def write_origin_file(files, dir_path):
    """Writes original filenames to the origin.txt file in the directory."""
    with open(f'{dir_path}/origin.txt', 'a') as origin_file:
        for filename in files:
            sanitized_name = sanitize_filename(filename)
            if sanitized_name != filename:
                origin_file.write(f'{sanitized_name}:\n{filename}\n')

async def upload_files(files, dir_path, url, headers, max_retries=3):
    """Uploads each file in `files` to the specified URL with custom headers. Retries on failure."""
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=3600)) as session:  # 1 hour timeout
        for filename in files:
            file_path = os.path.join(dir_path, filename)
            file_size = os.path.getsize(file_path)
            
            for attempt in range(max_retries):
                try:
                    async with aiofiles.open(file_path, 'rb') as file:
                        logging.info(f'Uploading: {filename} (Attempt {attempt + 1}/{max_retries})')
                        
                        # Use a stream to read and upload the file in chunks
                        async def file_sender():
                            chunk_size = 8192  # 8KB chunks
                            while True:
                                chunk = await file.read(chunk_size)
                                if not chunk:
                                    break
                                yield chunk
                        
                        headers['Content-Length'] = str(file_size)
                        async with session.post(f'{url}{sanitize_filename(filename)}', 
                                                headers=headers, 
                                                data=file_sender()) as response:
                            if response.status in [200, 201]:  # Both 200 and 201 indicate success
                                logging.info(f'Successfully uploaded: {filename}')
                                break  # Exit retry loop if successful
                            else:
                                logging.error(f'Failed to upload {filename}. Status: {response.status}')
                                if attempt == max_retries - 1:
                                    raise Exception(f"Failed to upload {filename} after {max_retries} attempts. Status: {response.status}")
                
                except asyncio.TimeoutError:
                    logging.error(f"Timeout while uploading {filename} (Attempt {attempt + 1}/{max_retries})")
                    if attempt == max_retries - 1:
                        raise Exception(f"Failed to upload {filename} after {max_retries} attempts due to timeout")
                
                except Exception as e:
                    logging.error(f"Error uploading {filename}: {str(e)} (Attempt {attempt + 1}/{max_retries})")
                    if attempt == max_retries - 1:
                        raise

    # clean-up(deleting) the queue directory
    logging.info(f'Deleting "{dir_path}" directory')
    shutil.rmtree(dir_path)

# Rate limiting decorator
def rate_limit(max_calls, period):
    calls = []
    def decorator(func):
        async def wrapper(*args, **kwargs):
            now = time.time()
            calls[:] = [c for c in calls if c > now - period]
            if len(calls) >= max_calls:
                raise Exception("Rate limit exceeded")
            calls.append(now)
            return await func(*args, **kwargs)
        return wrapper
    return decorator

class QueueManager:
    def __init__(self, max_concurrent=3):
        self.queues = {}  # Dictionary to store queues for each server
        self.active_processes = {}  # Dictionary to store active processes for each server
        self.max_concurrent = max_concurrent
        self.lock = asyncio.Lock()
        self.load_queue_state()

    def get_next_queue_num(self, server_id):
        existing_nums = set(int(d.split('_')[-1]) for d in os.listdir() if d.startswith(f'q_{server_id}_') and d.split('_')[-1].isdigit())
        next_num = 1
        while next_num in existing_nums:
            next_num += 1
        return next_num

    @rate_limit(max_calls=5, period=10)
    async def add_to_queue(self, interaction, url):
        server_id = interaction.guild_id
        channel_id = interaction.channel_id
        async with self.lock:
            if server_id not in self.queues:
                self.queues[server_id] = deque()
            if server_id not in self.active_processes:
                self.active_processes[server_id] = {}
            
            queue_num = self.get_next_queue_num(server_id)
            self.queues[server_id].append((queue_num, interaction.user.id, url, channel_id))
            self.save_queue_state()
        await self.process_queue(server_id)

    async def process_queue(self, server_id):
        async with self.lock:
            while len(self.active_processes[server_id]) < self.max_concurrent and self.queues[server_id]:
                queue_num, user_id, url, channel_id = self.queues[server_id].popleft()
                self.active_processes[server_id][queue_num] = asyncio.create_task(self.run_archival(server_id, queue_num, user_id, url, channel_id))
            self.save_queue_state()

    async def run_archival(self, server_id, queue_num, user_id, url, channel_id):
        dir_path = f'q_{server_id}_{queue_num}'
        temp_dir = f'temp_q_{server_id}_{queue_num}'
        start_time = time.time()
        archival_complete = False

        try:
            channel = client.get_channel(channel_id)
            if channel is None:
                logging.error(f"Cannot find channel with ID {channel_id}")
                return

            progress_message = await channel.send(f'[q{queue_num}]: Starting archival process...')

            # Start the archival process in a separate task
            archival_task = asyncio.create_task(start_archival(url, queue_num, server_id))

            # Update progress message every 10 seconds
            while not archival_complete:
                try:
                    await asyncio.sleep(10)
                    elapsed_time = int(time.time() - start_time)
                    await progress_message.edit(content=f'[q{queue_num}]: Archival in progress... (Elapsed time: {elapsed_time} seconds)')

                    # Check if the archival task is done
                    if archival_task.done():
                        archival_complete = True
                        _, archiver_stdout = archival_task.result()
                        logging.info(f"Archival process output: {archiver_stdout}")

                except asyncio.CancelledError:
                    archival_task.cancel()
                    await progress_message.edit(content=f'[q{queue_num}]: Archival process was cancelled.')
                    return

            await progress_message.edit(content=f'[q{queue_num}]: Archival process finished.')

            files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
            write_origin_file(files, dir_path)

            unique_id = generate_id(32)
            upload_url = f'https://filebin.net/{unique_id}/'
            headers = {
                'Application': 'application/json',
                'cid': unique_id,
                'Content-Type': 'application/octet-stream'
            }

            await progress_message.edit(content=f'[q{queue_num}]: Uploading files...')
            await upload_files(files, dir_path, upload_url, headers)

            await channel.send(f'<@{user_id}> [q{queue_num}]: Archive complete! Here are the links:\n'
                               f'Bin URL: <https://filebin.net/{unique_id}>\n'
                               f'Download ZIP: <https://filebin.net/archive/{unique_id}/zip>\n'
                               f'QR Code: https://filebin.net/qr/{unique_id}')

        except Exception as e:
            logging.error(f"Error in archival process for queue {queue_num}: {str(e)}")
            if 'progress_message' in locals():
                await progress_message.edit(content=f'[q{queue_num}]: An error occurred during the archival process.')
            else:
                logging.error(f"Could not send error message to channel {channel_id}")

        finally:
            # Cleanup
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

            async with self.lock:
                self.active_processes[server_id].pop(queue_num, None)
            await self.process_queue(server_id)

    def save_queue_state(self):
        state = {
            'queues': {server_id: list(queue) for server_id, queue in self.queues.items()},
            'active_processes': {server_id: {k: (v.get_name(), v.get_coro().cr_frame.f_locals['user_id'], v.get_coro().cr_frame.f_locals['url'], v.get_coro().cr_frame.f_locals['channel_id']) 
                             for k, v in processes.items()} 
                             for server_id, processes in self.active_processes.items()}
        }
        with open('queue_state.json', 'w') as f:
            json.dump(state, f)

    def load_queue_state(self):
        if os.path.exists('queue_state.json'):
            with open('queue_state.json', 'r') as f:
                state = json.load(f)
            self.queues = {server_id: deque(queue) for server_id, queue in state['queues'].items()}
            for server_id, processes in state['active_processes'].items():
                self.active_processes[server_id] = {}
                for queue_num, (_, user_id, url, channel_id) in processes.items():
                    self.active_processes[server_id][queue_num] = asyncio.create_task(self.run_archival(server_id, queue_num, user_id, url, channel_id))

# Create a global instance of QueueManager
queue_manager = QueueManager()

@tree.command(name="archive", description="Archive a YouTube livestream")
async def archive(interaction: discord.Interaction, url: str):
    await interaction.response.defer()
    try:
        await queue_manager.add_to_queue(interaction, url)
        await interaction.followup.send("Your archival request has been added to the queue. You'll be notified when it starts processing.")
    except Exception as e:
        await interaction.followup.send(f"Error adding to queue: {str(e)}")

@client.event
async def on_ready():
    await tree.sync()
    logging.info(f'Logged in as {client.user}')

async def main():
    try:
        await client.start(TOKEN)
    except KeyboardInterrupt:
        # Handle graceful shutdown
        await client.close()
    except Exception as e:
        logging.error(f"Error starting client: {str(e)}")
    finally:
        # Ensure the client is closed
        if not client.is_closed():
            await client.close()

if __name__ == "__main__":
    asyncio.run(main())
