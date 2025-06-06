import asyncio
from pyrogram import Client
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Bot configuration
API_ID = "7813390"  # Replace with your API ID
API_HASH = "1faadd9cc60374bca1e88c2f44e3ee2f"  # Replace with your API Hash
BOT_TOKEN = "2015610141:AAGXO7HTsrIUeDFfjyHhZHR6WqLA08Lg07I"  # Replace with your bot token

# MongoDB configuration
MONGO_URL = "mongodb+srv://TheAutoFilterBot:TheAutoFilterBot@cluster0.d7c2e.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"  # Replace with your MongoDB URL
DB_NAME = "TheAutoFilterBot"  # Replace with your database name
COLLECTION_NAME = "TheAutoFilterBot"  # Your Media collection name

# Target chat ID where files will be uploaded
TARGET_CHAT_ID = "-1002478164714"  # Replace with target chat ID

class MongoToTelegramUploader:
    def __init__(self):
        self.bot = Client(
            "mongo_uploader_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN
        )
        self.mongo = AsyncIOMotorClient(MONGO_URL)
        self.db = self.mongo[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]
        
    async def check_connection(self):
        """Check MongoDB connection and print database info"""
        try:
            # List all databases
            databases = await self.mongo.list_database_names()
            logger.info(f"Available databases: {databases}")
            
            # List all collections in the selected database
            collections = await self.db.list_collection_names()
            logger.info(f"Collections in {DB_NAME}: {collections}")
            
            # Get a sample document
            sample_doc = await self.collection.find_one()
            if sample_doc:
                logger.info("Sample document structure:")
                logger.info(sample_doc)
            else:
                logger.warning(f"No documents found in collection {COLLECTION_NAME}")
                
            return True
        except Exception as e:
            logger.error(f"MongoDB connection error: {str(e)}")
            return False
        
    async def count_documents(self):
        """Count total documents in collection"""
        return await self.collection.count_documents({})
        
    async def upload_files(self):
        """Upload all files from MongoDB to Telegram"""
        try:
            # Start the bot
            await self.bot.start()
            
            # Check MongoDB connection first
            if not await self.check_connection():
                logger.error("Failed to connect to MongoDB. Please check your connection settings.")
                return
            
            # Get total number of documents
            total_docs = await self.count_documents()
            logger.info(f"Found {total_docs} documents to upload")
            
            if total_docs == 0:
                logger.warning("No documents found to upload. Please check your database and collection names.")
                return
            
            # Initialize counters
            success = 0
            failed = 0
            
            # Create progress bar
            pbar = tqdm(total=total_docs, desc="Uploading files")
            
            async for doc in self.collection.find({}):
                try:
                    file_id = doc.get('file_id')
                    file_name = doc.get('file_name', 'Unknown')
                    file_caption = doc.get('caption', '')
                    
                    if not file_id:
                        logger.warning(f"No file_id found for document: {file_name}")
                        logger.info(f"Document structure: {doc}")
                        failed += 1
                        continue
                        
                    # Upload file to Telegram
                    await self.bot.send_cached_media(
                        chat_id=TARGET_CHAT_ID,
                        file_id=file_id,
                        caption=file_caption
                    )
                    
                    success += 1
                    logger.info(f"Successfully uploaded: {file_name}")
                    
                except Exception as e:
                    failed += 1
                    logger.error(f"Failed to upload {file_name}: {str(e)}")
                    logger.error(f"Document that failed: {doc}")
                    
                finally:
                    pbar.update(1)
                    
                # Add small delay to avoid hitting rate limits
                await asyncio.sleep(0.5)
            
            pbar.close()
            
            # Print final statistics
            logger.info(f"""
            Upload Complete!
            Total files: {total_docs}
            Successfully uploaded: {success}
            Failed: {failed}
            """)
            
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            
        finally:
            await self.bot.stop()
            self.mongo.close()

async def main():
    uploader = MongoToTelegramUploader()
    await uploader.upload_files()

if __name__ == "__main__":
    # Run the uploader
    asyncio.run(main()) 
