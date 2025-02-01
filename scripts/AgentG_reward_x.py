import os
import json
import asyncio
import sqlite3
import requests
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from tweety import Twitter
from web3 import Web3

# global constants
WINDOW_IN_H = 24
TOTAL_REWARD = 10
AMOUNT_HELD = 100
BOT_USERNAME = "testelizax2"
EXCLUDED_USERS = ["user1", "user2"]  # Usernames instead of IDs

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self, db_name='gmetisx.db'):
        self.db_name = db_name
        self.setup_database()

    def setup_database(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Create rewards table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS rewards (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            post_id TEXT,
            wa TEXT,
            balance INT,
            reward INT,
            tx TEXT,
            date DATE,
            reward_round INTEGER
        )
        ''')

        # Create waMap table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS waMap (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT,
            username TEXT,
            wa TEXT,
            date DATE
        )
        ''')
        
        conn.commit()
        conn.close()

    def get_last_winner(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('''
        SELECT username 
        FROM rewards 
        ORDER BY date DESC, ID DESC 
        LIMIT 1
        ''')
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def get_latest_wa(self, username):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('SELECT wa FROM waMap WHERE username = ? ORDER BY ID DESC LIMIT 1', (username,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def get_next_reward_round(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(reward_round) FROM rewards')
        result = cursor.fetchone()[0]
        conn.close()
        return (result or 0) + 1

    def add_reward_entry(self, username, post_id, reward_round=None, wa=None, balance=None, reward=None, tx=None):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        current_date = datetime.now().date()
        if not reward_round:
            reward_round = self.get_next_reward_round()
        
        existing = None
        if tx:
            cursor.execute('''
                UPDATE rewards 
                SET tx = ? 
                WHERE reward_round = ?
            ''', (tx, reward_round))
        else:
            cursor.execute('SELECT ID FROM rewards WHERE reward_round = ?', (reward_round,))
            existing = cursor.fetchone()
            if existing:
                cursor.execute('''
                    UPDATE rewards 
                    SET wa = ?, balance = ?, reward = ?, date = ?
                    WHERE reward_round = ?
                ''', (wa, balance, reward, current_date, reward_round))
            else:
                cursor.execute('''
                    INSERT INTO rewards (username, post_id, wa, balance, reward, tx, date, reward_round)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (username, post_id, wa, balance, reward, tx, current_date, reward_round))
        
        conn.commit()
        conn.close()
        return reward_round

    def add_pending_reward(self, username, post_id):
        reward_round = self.get_next_reward_round()
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        current_date = datetime.now().date()
        
        cursor.execute('''
        INSERT INTO rewards (username, post_id, date, reward_round)
        VALUES (?, ?, ?, ?)
        ''', (username, post_id, current_date, reward_round))
        
        conn.commit()
        conn.close()
        return reward_round

    def get_pending_rewards(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT reward_round, username, post_id, date 
        FROM rewards 
        WHERE wa IS NULL AND tx IS NULL
        ORDER BY date ASC, reward_round ASC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        return results

class XManager:
    def __init__(self):
        self.app = Twitter('session')
        self.bot_username = BOT_USERNAME
        
    async def initialize(self):
        await self.app.sign_in(os.getenv('TWITTER_USERNAME'), os.getenv('TWITTER_PASSWORD'))
        logger.info("Successfully signed in")

    async def get_recent_mentions(self, hours):
        hours_ago = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        try:
            # Get mentions using search
            search = await self.app.search(f"@{self.bot_username}")
            
            # Log the raw response for debugging
            logger.info(f"Raw search response: {search}")
            
            recent_mentions = []
            
            if hasattr(search, 'results'):
                logger.info(f"Search results: {search.results}")
                for tweet in search.results:
                    try:
                        tweet_time = tweet.created_on
                        
                        if tweet_time >= hours_ago:
                            recent_mentions.append(tweet)
                            logger.info(f"Found mention from @{tweet.author.username} at {tweet_time}")
                        else:
                            break
                    except Exception as e:
                        logger.error(f"Error processing tweet: {e}")
                        continue
            
            logger.info(f"Found {len(recent_mentions)} recent mentions")
            return recent_mentions
            
        except Exception as e:
            logger.error(f"Error searching mentions: {e}")
            return []

    def get_most_engaging_post(self, posts, excluded_users):
        if not posts:
            return None, None
        
        try:
            # Sort posts by engagement (likes + retweets + replies)
            sorted_posts = sorted(
                posts,
                key=lambda x: (
                    getattr(x, 'likes', 0) + 
                    getattr(x, 'retweet_counts', 0) + 
                    getattr(x, 'reply_counts', 0)
                ),
                reverse=True
            )
            
            for post in sorted_posts:
                try:
                    author = post.author.username
                    if author not in excluded_users:
                        logger.info(f"Selected winning post from @{author}")
                        return post, author
                except Exception as e:
                    logger.error(f"Error getting author from post: {e}")
                    continue
            
            logger.info("No eligible posts found after filtering")
            return None, None
            
        except Exception as e:
            logger.error(f"Error in get_most_engaging_post: {e}")
            return None, None

    async def reply_to_tweet(self, tweet_id, message):
        try:
            # Try different parameter names that might be used by the API
            try:
                # First attempt with reply_to_tweet_id
                response = await self.app.create_tweet(
                    text=message,
                    reply_to_tweet_id=tweet_id
                )
            except TypeError:
                try:
                    # Second attempt with reply_to
                    response = await self.app.create_tweet(
                        text=message,
                        reply_to=tweet_id
                    )
                except TypeError:
                    # Final attempt with conversation_id
                    response = await self.app.create_tweet(
                        text=message,
                        conversation_id=tweet_id
                    )
            
            if response:
                logger.info(f"Successfully replied to tweet {tweet_id}")
                return response.id
            else:
                logger.error(f"Failed to reply to tweet {tweet_id} - no response")
                return None
                
        except Exception as e:
            logger.error(f"Failed to send tweet: {e}")
            return None
               
class Web3Manager:
    def __init__(self):
        self.web3 = Web3(Web3.HTTPProvider('https://andromeda.metis.io/?owner=1088'))
        self.private_key = os.getenv('PRIVATE_KEY')
        
        # Load ABI
        with open('assets/ABI.json', 'r') as abi_file:
            self.gmetis_abi = json.load(abi_file)
        
        self.gmetis_contract_address = '0xFbe0F778e3c1168bc63d7b6F880Ec0d5F9E524E6'
        self.contract = self.web3.eth.contract(
            address=self.gmetis_contract_address, 
            abi=self.gmetis_abi
        )

    def get_gmetis_balance(self, wallet_address):
        url = f"https://andromeda-explorer.metis.io/api/v2/addresses/{wallet_address}/token-balances"
        response = requests.get(url)
        
        if response.status_code == 200:
            token_balances = response.json()
            for token_balance in token_balances:
                token = token_balance.get("token", {})
                if token.get("symbol") == "gMetis":
                    balance = token_balance.get("value")
                    decimals = int(token.get("decimals", 18))
                    return int(int(balance) / (10 ** decimals))  # Convert to integer
        return 0

    def send_gmetis(self, to_address, amount):
        if not self.web3.is_connected():
            raise Exception("Failed to connect to the network")

        from_address = self.web3.eth.account.from_key(self.private_key).address
        decimals = self.contract.functions.decimals().call()
        amount_in_wei = int(amount * (10 ** decimals))
        nonce = self.web3.eth.get_transaction_count(from_address)

        tx = self.contract.functions.transfer(to_address, amount_in_wei).build_transaction({
            'chainId': 1088,
            'gas': 2000000,
            'gasPrice': self.web3.to_wei('5', 'gwei'),
            'nonce': nonce
        })

        signed_tx = self.web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.raw_transaction)
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt.transactionHash.hex()
    
def calculate_reward(total_reward=TOTAL_REWARD, amount_held=AMOUNT_HELD, tokens_held=0):
    if tokens_held >= amount_held:
        return int(total_reward)
    elif tokens_held == 0:
        return int(total_reward * 0.2)
    else:
        return int(total_reward * (0.2 + (tokens_held / amount_held) * 0.8))

async def process_pending_rewards(db_manager, x_manager, web3_manager):
    pending_rewards = db_manager.get_pending_rewards()
    
    for reward_round, username, post_id, date in pending_rewards:
        wallet_address = db_manager.get_latest_wa(username)
        if not wallet_address:
            continue
            
        balance = web3_manager.get_gmetis_balance(wallet_address)
        reward_amount = calculate_reward(TOTAL_REWARD, AMOUNT_HELD, balance)
        
        db_manager.add_reward_entry(
            username=username,
            post_id=post_id,
            wa=wallet_address,
            balance=balance,
            reward=reward_amount,
            reward_round=reward_round
        )
        
        tx_hash = web3_manager.send_gmetis(wallet_address, reward_amount)
        
        db_manager.add_reward_entry(
            username=username,
            post_id=post_id,
            tx=tx_hash,
            reward_round=reward_round
        )
        
        await x_manager.reply_to_tweet(
            post_id,
            f"🎯 Reward Round #{reward_round} - Retroactive Payout 🎯\n\n"
            f"✨ Processing their previously won reward for @{username} \n"
            f"🎁 You've received {reward_amount} gMetis! \n"
            f"📅 Original win date: {date}\n"
            f"🔗 Transaction: https://andromeda-explorer.metis.io/tx/0x{tx_hash}\n\n"
            f"🚀 Better late than never! Keep engaging! 🚀"
        )


async def main():
    db_manager = DatabaseManager()
    x_manager = XManager()
    web3_manager = Web3Manager()
    
    try:
        # Initialize Twitter client
        await x_manager.initialize()
        
        last_winner = db_manager.get_last_winner()
        if last_winner:
            EXCLUDED_USERS.append(last_winner)
        
        # Get recent mentions
        mentions = await x_manager.get_recent_mentions(WINDOW_IN_H)
        
        # Handle quiet period
        if not mentions:
            logger.info("No recent mentions found")
            return
        
        # Select winning post
        winning_post, winner_username = x_manager.get_most_engaging_post(mentions, EXCLUDED_USERS)
        if not winning_post:
            logger.info("No eligible winning post found")
            return
        
        logger.info(f"Selected winner: {winner_username}")
        
        # Get wallet address for winner
        wallet_address = db_manager.get_latest_wa(winner_username)
        reward_round = db_manager.get_next_reward_round()

        if not wallet_address:
            reward_round = db_manager.add_pending_reward(winner_username, winning_post.id)
            
            await x_manager.reply_to_tweet(
                winning_post.id,
                f"🎯 Reward Round #{reward_round}\n\n"
                f"🎉 Congratulations @{winner_username}! 🎉\n"
                f"✨ Your post has been selected for a reward! \n"
                f"📝 Please DM @{BOT_USERNAME} to register your wallet address.\n"
                f"💫 Your reward will be processed automatically once registered!"
            )
            logger.info(f"Added pending reward for {winner_username}")
        else:
            balance = web3_manager.get_gmetis_balance(wallet_address)
            reward_amount = calculate_reward(TOTAL_REWARD, AMOUNT_HELD, balance)
            
            reward_round = db_manager.add_reward_entry(
                username=winner_username,
                post_id=winning_post.id,
                wa=wallet_address,
                balance=balance,
                reward=reward_amount
            )
            
            tx_hash = web3_manager.send_gmetis(wallet_address, reward_amount)
            logger.info(f"Sent {reward_amount} gMetis to {wallet_address}, tx: {tx_hash}")
            
            db_manager.add_reward_entry(
                username=winner_username,
                post_id=winning_post.id,
                tx=tx_hash,
                reward_round=reward_round
            )
            
            await x_manager.reply_to_tweet(
                winning_post.id,
                f"🎯 Reward Round #{reward_round}\n\n"
                f"🎉 Congratulations @{winner_username}! 🎉\n"
                f"✨ Thank you for engaging with us! \n"
                f"🎁 You've received {reward_amount} gMetis! \n"
                f"🔗 Transaction: https://andromeda-explorer.metis.io/tx/0x{tx_hash}\n\n"
                f"🚀 Keep engaging and stay awesome! 🚀"
            )
        
        # Process any pending rewards
        await process_pending_rewards(db_manager, x_manager, web3_manager)
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())