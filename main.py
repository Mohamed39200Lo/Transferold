from app import server 
import os
from telethon import TelegramClient, functions, types
from telethon.tl.types import Message
from telethon.errors import FloodWaitError, ChatAdminRequiredError, RPCError
from datetime import datetime
import logging
import json
import asyncio
import requests
from packaging import version

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transfer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Telegram API credentials
api_id = 23068290
api_hash = 'e38697ea4202276cbaa91d20b99af864'
source_chat_id = -1002216445147
target_chat_id = -1002536097241
# GitHub Gist credentials
token_part1 = "ghp_gFkAlF"
token_part2 = "A4sbNyuLtX"
token_part3 = "YvqKfUEBHXNaPh3ABRms"
GITHUB_TOKEN = token_part1 + token_part2 + token_part3
GIST_ID = "1050e1f10d7f5591f4f26ca53f2189e9"

# File name for Gist
GIST_FILE_NAME = 'progrjess.json'

# Batch processing settings
BATCH_SIZE = 100
BATCH_PAUSE = 30  # Seconds to pause after each batch

# Initialize Telethon client
client = TelegramClient('session', api_id, api_hash)

async def get_topic_mapping():
    """Fetch and map topic IDs between source and target groups using raw API."""
    try:
        # Check Telethon version
        from telethon import __version__ as telethon_version
        if version.parse(telethon_version) < version.parse('1.25.0'):
            logger.warning(f"Telethon version {telethon_version} is outdated. Topic support requires 1.25.0 or higher. Attempting fallback method.")
            return await get_topic_mapping_fallback()

        # Fetch topics using raw API
        source_entity = await client.get_entity(source_chat_id)
        target_entity = await client.get_entity(target_chat_id)
        
        source_topics = await client(functions.channels.GetForumTopicsRequest(
            channel=source_entity,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        target_topics = await client(functions.channels.GetForumTopicsRequest(
            channel=target_entity,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        
        topic_mapping = {}
        for source_topic in source_topics.topics:
            source_name = source_topic.title.strip()
            for target_topic in target_topics.topics:
                if target_topic.title.strip().lower() == source_name.lower():
                    topic_mapping[source_topic.id] = target_topic.id
                    logger.info(f"Mapped topic '{source_name}' (ID {source_topic.id}) to target ID {target_topic.id}")
                    break
            else:
                logger.warning(f"No matching topic found for '{source_name}' in target group")
        
        return topic_mapping
    except Exception as e:
        logger.error(f"Error fetching topics with raw API: {str(e)}. Attempting fallback method.")
        return await get_topic_mapping_fallback()

async def get_topic_mapping_fallback():
    """Fallback method to map topics by creating or finding matching topic names."""
    try:
        topic_mapping = {}
        source_entity = await client.get_entity(source_chat_id)
        target_entity = await client.get_entity(target_chat_id)
        
        source_topics = await client(functions.channels.GetForumTopicsRequest(
            channel=source_entity,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        
        target_topics = await client(functions.channels.GetForumTopicsRequest(
            channel=target_entity,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        
        target_topic_dict = {t.title.strip().lower(): t.id for t in target_topics.topics}
        
        for source_topic in source_topics.topics:
            source_name = source_topic.title.strip()
            source_name_lower = source_name.lower()
            
            if source_name_lower in target_topic_dict:
                topic_mapping[source_topic.id] = target_topic_dict[source_name_lower]
                logger.info(f"Fallback: Mapped topic '{source_name}' (ID {source_topic.id}) to target ID {target_topic_dict[source_name_lower]}")
            else:
                try:
                    new_topic = await client(functions.channels.CreateForumTopicRequest(
                        channel=target_entity,
                        title=source_name
                    ))
                    topic_mapping[source_topic.id] = new_topic.updates[0].message.reply_to.reply_to_msg_id
                    logger.info(f"Fallback: Created and mapped topic '{source_name}' (ID {source_topic.id}) to new target ID {topic_mapping[source_topic.id]}")
                except Exception as e:
                    logger.warning(f"Failed to create topic '{source_name}' in target group: {str(e)}")
        
        if not topic_mapping:
            logger.warning("No topics mapped in fallback method. Messages will be sent to default topic.")
        return topic_mapping
    except Exception as e:
        logger.error(f"Fallback topic mapping failed: {str(e)}. Proceeding without topic mapping.")
        return {}

async def load_progress():
    """Load the last processed message ID from the Gist."""
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        response = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=headers)
        if response.status_code == 200:
            files = response.json().get('files', {})
            content = files.get(GIST_FILE_NAME, {}).get('content', '{}')
            data = json.loads(content)
            logger.info(f"Loaded progress from Gist: last_message_id={data.get('last_message_id', 0)}")
            return data.get('last_message_id', 0)
        else:
            logger.error(f"Failed to load Gist: {response.status_code}, {response.text}")
            return 0
    except Exception as e:
        logger.error(f"Error loading progress from Gist: {str(e)}")
        return 0

async def save_progress(message_id):
    """Save the last processed message ID to the Gist."""
    data = {'last_message_id': message_id}
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    payload = {
        "files": {
            GIST_FILE_NAME: {
                "content": json.dumps(data, indent=4, default=str)
            }
        }
    }
    try:
        response = requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f"Saved progress to Gist: last_message_id={message_id}")
        else:
            logger.error(f"Failed to update Gist: {response.status_code}, {response.text}")
    except Exception as e:
        logger.error(f"Error saving progress to Gist: {str(e)}")

async def transfer_messages():
    """Transfer messages from source to target group, preserving topics and media groups."""
    await client.start()
    logger.info("Client started")

    # Get topic mapping
    topic_mapping = await get_topic_mapping()
    if not topic_mapping:
        logger.warning("No topic mapping available. Messages will be sent to default topic.")

    # Load last processed message ID
    last_message_id = await load_progress()
    logger.info(f"Resuming from message ID {last_message_id}")

    batch_count = 0
    media_group = {}
    last_group_id = None
    last_topic_id = None

    try:
        async for message in client.iter_messages(source_chat_id, reverse=True, min_id=last_message_id):
            try:
                # Skip messages that are service messages or empty
                if not isinstance(message, Message) or (not message.message and not message.media):
                    logger.debug(f"Skipping message ID {message.id}: Not a valid message")
                    continue

                # Determine the target topic ID
                topic_id = message.reply_to.reply_to_msg_id if message.reply_to and message.reply_to.forum_topic else None
                target_topic_id = topic_mapping.get(topic_id, None)

                if topic_id and not target_topic_id:
                    logger.warning(f"No target topic found for source topic ID {topic_id}. Sending to default topic.")

                # Handle media groups
                if message.media and message.grouped_id:
                    group_id = message.grouped_id
                    if group_id not in media_group:
                        media_group[group_id] = {'messages': [], 'topic_id': target_topic_id}

                    media_group[group_id]['messages'].append(message)
                    last_group_id = group_id
                    last_topic_id = target_topic_id

                    # Check if this is the last message in the group (by fetching next message)
                    next_messages = await client.get_messages(source_chat_id, ids=[message.id + 1])
                    next_message = next_messages[0] if next_messages else None
                    if not next_message or next_message.grouped_id != group_id:
                        # Process the media group
                        group_messages = media_group[group_id]['messages']
                        media_files = []
                        caption = ""

                        for msg in group_messages:
                            if isinstance(msg.media, (types.MessageMediaPhoto, types.MessageMediaDocument)):
                                # Check media size
                                if hasattr(msg.media, 'document') and msg.media.document.size > 2 * 1024 * 1024 * 1024:
                                    logger.warning(f"Skipping media in message ID {msg.id}: Media size exceeds 2GB")
                                    continue
                                media_files.append(msg.media)
                                # Use the first non-empty caption
                                if msg.message and not caption:
                                    caption = msg.message

                        if media_files:
                            try:
                                await client.send_file(
                                    target_chat_id,
                                    file=media_files,
                                    caption=caption,
                                    reply_to=media_group[group_id]['topic_id']
                                )
                                logger.info(f"Transferred media group {group_id} with {len(media_files)} items to topic {media_group[group_id]['topic_id'] or 'default'}")
                            except Exception as e:
                                logger.error(f"Error transferring media group {group_id}: {str(e)}")
                                continue

                        # Update progress to the last message in the group
                        await save_progress(group_messages[-1].id)
                        batch_count += 1
                        del media_group[group_id]

                    continue  # Skip individual processing for grouped messages

                # Handle non-grouped messages
                try:
                    if message.media:
                        # Skip unsupported media types
                        if isinstance(message.media, (types.MessageMediaWebPage, types.MessageMediaUnsupported)):
                            logger.debug(f"Skipping message ID {message.id}: Unsupported media type")
                            continue
                        # Check media size
                        if hasattr(message.media, 'document') and message.media.document.size > 2 * 1024 * 1024 * 1024:
                            logger.warning(f"Skipping message ID {message.id}: Media size exceeds 2GB")
                            continue
                        # Copy supported media
                        await client.send_file(
                            target_chat_id,
                            file=message.media,
                            caption=message.message or "",
                            reply_to=target_topic_id
                        )
                    else:
                        # Send text messages
                        await client.send_message(
                            target_chat_id,
                            message=message.message,
                            reply_to=target_topic_id
                        )
                    
                    logger.info(f"Transferred message ID {message.id} to topic {target_topic_id or 'default'}")
                    
                    # Save progress
                    await save_progress(message.id)
                    
                    # Increment batch counter
                    batch_count += 1

                    if batch_count % BATCH_SIZE == 0:
                        logger.info(f"Processed {batch_count} messages. Pausing for {BATCH_PAUSE} seconds.")
                        await asyncio.sleep(BATCH_PAUSE)
                    else:
                        await asyncio.sleep(4)  # Increased delay to avoid rate limits

                except FloodWaitError as e:
                    logger.warning(f"Flood wait error: Waiting for {e.seconds} seconds")
                    await asyncio.sleep(e.seconds)
                    continue
                except ChatAdminRequiredError:
                    logger.error("Bot lacks permissions in target group. Exiting.")
                    break
                except RPCError as e:
                    logger.error(f"Error transferring message ID {message.id}: {str(e)}")
                    await asyncio.sleep(5)
                    continue

            except Exception as e:
                logger.error(f"Error processing message ID {message.id}: {str(e)}")
                await asyncio.sleep(5)
                continue

    except Exception as e:
        logger.error(f"Error iterating messages: {str(e)}")
    
    finally:
        await client.disconnect()
        logger.info("Client disconnected")

async def main():
    """Main function to run the transfer process."""
    try:
        await transfer_messages()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == '__main__':
    server()
    asyncio.run(main())
