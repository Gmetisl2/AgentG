import os
import json
import asyncio
import sqlite3
import requests
import logging
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
from web3 import Web3
from dotenv import load_dotenv
import openai

# global constants
WINDOW_IN_H = 4
TOTAL_REWARD = 100
AMOUNT_HELD = 100
EXCLUDED_USERS = [5571930248, 7340946957]

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self, db_name='gmetis.db'):
        self.db_name = db_name
        self.setup_database()

    def setup_database(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Create rewards table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS rewards (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            userid TEXT,
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
            userID TEXT,
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
        SELECT userid 
        FROM rewards 
        ORDER BY date DESC, ID DESC 
        LIMIT 1
        ''')
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    
    def get_latest_wa(self, identifier, value):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        column = 'userID' if identifier == 'userID' else 'username'
        cursor.execute(f'SELECT wa FROM waMap WHERE {column} = ? ORDER BY ID DESC LIMIT 1', (value,))
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

    def add_reward_entry(self, userid, reward_round=None, wa=None, balance=None, reward=None, tx=None):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        current_date = datetime.now().date()
        if not reward_round:
            reward_round = self.get_next_reward_round()
        
        existing = None  # Initialize existing to None
        if tx:
            # Update existing entry with transaction hash
            cursor.execute('''
                UPDATE rewards 
                SET tx = ? 
                WHERE reward_round = ?
            ''', (tx, reward_round))
        else:
            # Check if entry exists for this reward round
            cursor.execute('SELECT ID FROM rewards WHERE reward_round = ?', (reward_round,))
            existing = cursor.fetchone()
            if existing:
                # Update existing entry
                cursor.execute('''
                    UPDATE rewards 
                    SET wa = ?, balance = ?, reward = ?, date = ?
                    WHERE reward_round = ?
                ''', (wa, balance, reward, current_date, reward_round))
            else:
                # Create new entry
                cursor.execute('''
                    INSERT INTO rewards (userid, wa, balance, reward, tx, date, reward_round)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (userid, wa, balance, reward, tx, current_date, reward_round))
        
        conn.commit()
        reward_id = cursor.lastrowid if not existing else existing[0]
        conn.close()
        return reward_round

    def add_pending_reward(self, userid):
        """Add a pending reward entry with just the userID"""
        reward_round = self.get_next_reward_round()
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        current_date = datetime.now().date()
        
        cursor.execute('''
        INSERT INTO rewards (userid, date, reward_round)
        VALUES (?, ?, ?)
        ''', (userid, current_date, reward_round))
        
        conn.commit()
        conn.close()
        return reward_round

    def get_pending_rewards(self):
        """Get all rewards entries that have userID but no wallet address"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT reward_round, userid, date 
        FROM rewards 
        WHERE wa IS NULL AND tx IS NULL
        ORDER BY date ASC, reward_round ASC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        return results


class TelegramManager:
    def __init__(self):
        self.api_id = os.getenv('TG_API_ID')
        self.api_hash = os.getenv('TG_API_HASH')
        self.bot_token = os.getenv('BOT_TOKEN')
        self.group_id = int(os.getenv('GROUP_ID'))
        self.client = TelegramClient('user', self.api_id, self.api_hash)

    async def get_recent_messages(self, hours):
        await self.client.start()
        messages = []
        hours_ago = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        group = await self.client.get_entity(self.group_id)
        async for message in self.client.iter_messages(group):
            if message.date >= hours_ago:
                messages.append({
                    "username": message.sender.username if message.sender else None,
                    "userid": message.sender_id,
                    "text": message.text,
                    "date": message.date.isoformat()
                })
        
        await self.client.disconnect()
        return messages

    async def send_message(self, message):
        await self.client.start()
        group = await self.client.get_entity(self.group_id)
        await self.client.send_message(group, message)
        await self.client.disconnect()

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

def calculate_reward(total_reward=250000, amount_held=250000, tokens_held=0):
    if tokens_held >= amount_held:
        return int(total_reward)
    elif tokens_held == 0:
        return int(total_reward * 0.2)
    else:
        return int(total_reward * (0.2 + (tokens_held / amount_held) * 0.8))

def get_most_engaging_member(messages, excluded_users):
    openai.api_key = os.getenv('OPENAI_API_KEY')
    
    conversation_history = ""
    for message in messages[:100]:  # Limit to last 100 messages
        if message['text']:
            conversation_history += f"{message['userid']}: {message['text']}\n"

    # Convert excluded_users list to string
    excluded_users_str = ', '.join(map(str, excluded_users))
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": f"You are a helpful assistant. Only provide a single number as the userid and nothing more. The following users cannot win and you cannot return these userids: ({excluded_users_str})"},
            {"role": "user", "content": f"Given the following conversation history, identify the most engaging member by returning their ID:\n\n{conversation_history}\nThe following users cannot win and you cannot return these userids: ({excluded_users_str})"}
        ],
        max_tokens=50,
        temperature=0.5
    )
    
    return response.choices[0].message['content'].strip()

async def process_pending_rewards(db_manager, tg_manager, web3_manager):
    """Process all pending rewards where wallet address is now available"""
    pending_rewards = db_manager.get_pending_rewards()
    
    for reward_round, userid, date in pending_rewards:
        # Check if wallet address is now available
        wallet_address = db_manager.get_latest_wa('userID', userid)
        if not wallet_address:
            continue
            
        # Get balance and calculate reward
        balance = web3_manager.get_gmetis_balance(wallet_address)
        reward_amount = calculate_reward(TOTAL_REWARD, AMOUNT_HELD, balance)
        
        # Update the existing reward entry with wallet and balance
        db_manager.add_reward_entry(
            userid=userid,
            wa=wallet_address,
            balance=balance,
            reward=reward_amount,
            reward_round=reward_round
        )
        
        # Send reward
        tx_hash = web3_manager.send_gmetis(wallet_address, reward_amount)
        
        # Update with transaction hash
        db_manager.add_reward_entry(
            userid=userid,
            tx=tx_hash,
            reward_round=reward_round
        )
        
        # Get username for notification
        messages = await tg_manager.get_recent_messages(24)  # Look in last 24h to find username
        user_info = next((m for m in messages if str(m['userid']) == str(userid)), None)
        username = user_info['username'] if user_info else str(userid)
        
        # Send retrospective reward announcement
        await tg_manager.send_message(
            f"🎯 Reward Round #{reward_round} - Retrospective Payout 🎯\n\n"
            f"🎉 @{username} has registered their wallet address! 🎉\n"
            f"✨ Processing their previously won reward... \n"
            f"🎁 They've received {reward_amount} gMetis! \n"
            f"📅 Original win date: {date}\n"
            f"🔗 Transaction: https://andromeda-explorer.metis.io/tx/0x{tx_hash}\n\n"
            f"🚀 Better late than never! Keep vibing! 🚀"
        )

async def main():

    
    # Initialize managers
    db_manager = DatabaseManager()
    tg_manager = TelegramManager()
    web3_manager = Web3Manager()
    
    try:
        # Get last winner and add to excluded users
        last_winner = db_manager.get_last_winner()
        if last_winner:
            EXCLUDED_USERS.append(int(last_winner))
        
        # 1. Get recent messages
        messages = await tg_manager.get_recent_messages(WINDOW_IN_H)
        
        # Save messages to file
        with open('messages_lh.json', 'w', encoding='utf-8') as f:
            json.dump(messages, f, ensure_ascii=False, indent=4)
        
        # 2. Handle quiet period
        if not messages:
            await tg_manager.send_message("It's awfully quiet here. Anyone is vibing? 🎵")
            return
        
        # 3. Select winner and start reward process
        winner_id = get_most_engaging_member(messages, EXCLUDED_USERS)
        winner_info = next((m for m in messages if str(m['userid']) == str(winner_id)), None)
        
        if not winner_info:
            logger.error("Winner not found in messages")
            await tg_manager.send_message("Seems like we don't have much of a crowd here huh? ")
            return
            return
        
        # 4. Get wallet address for current winner
        wallet_address = db_manager.get_latest_wa('userID', winner_id)
        reward_round = db_manager.get_next_reward_round()

        if not wallet_address:
            # Add pending reward entry
            reward_round = db_manager.add_pending_reward(winner_id)
            
            # Notify user to register wallet
            await tg_manager.send_message(
                f"🎯 Reward Round #{reward_round}\n\n"
                f"🎉 Congratulations @{winner_info['username']}! 🎉\n"
                f"✨ You've been selected for a reward! \n"
                f"📝 Please message @AgentG_gmetisbot to register your wallet address.\n"
                f"💫 Your reward will be processed automatically once registered!"
            )
        else:
            # Process current winner's reward
            balance = web3_manager.get_gmetis_balance(wallet_address)
            reward_amount = calculate_reward(TOTAL_REWARD, AMOUNT_HELD, balance)
            
            # Create/update reward entry
            reward_round = db_manager.add_reward_entry(
                userid=winner_id,
                wa=wallet_address,
                balance=balance,
                reward=reward_amount,
                reward_round=reward_round
            )
            
            # Send reward
            tx_hash = web3_manager.send_gmetis(wallet_address, reward_amount)
            
            # Update with transaction hash
            db_manager.add_reward_entry(
                userid=winner_id,
                tx=tx_hash,
                reward_round=reward_round
            )
            
            # Send current round winner announcement
            await tg_manager.send_message(
                f"🎯 Reward Round #{reward_round}\n\n"
                f"🎉 Congratulations @{winner_info['username']}! 🎉\n"
                f"✨ Thank you for vibing with us! \n"
                f"🎁 You've received {reward_amount} gMetis! \n"
                f"🔗 Transaction: https://andromeda-explorer.metis.io/tx/0x{tx_hash}\n\n"
                f"🚀 Keep shining and stay awesome! 🚀"
            )
            
        # 5. Process any pending rewards after current round
        await process_pending_rewards(db_manager, tg_manager, web3_manager)
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())