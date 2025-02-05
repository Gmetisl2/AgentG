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
from telegram import Bot

#Azure
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Date, select, desc
from sqlalchemy.orm import declarative_base, Session



# global constants
WINDOW_IN_H = 2
TOTAL_REWARD = 50000
AMOUNT_HELD = 1000000
EXCLUDED_USERS = [7340946957,5571930248, 7843080080, 7694522043, 609517172, 301429358]

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

Base = declarative_base()

class DatabaseManager:
    def __init__(self):
        # Connection details
        server = os.getenv('AZURE_SQL_SERVER')
        database = os.getenv('AZURE_SQL_DATABASE')
        username = os.getenv('AZURE_SQL_USERNAME')
        password = os.getenv('AZURE_SQL_PASSWORD')
        
        # Create connection string
        self.connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server'
        self.engine = create_engine(self.connection_string)
        self.setup_database()

    def setup_database(self):
        # Create tables using SQLAlchemy
        metadata = MetaData()
        
        # Define rewards table
        self.rewards = Table(
            'rewards', metadata,
            Column('ID', Integer, primary_key=True, autoincrement=True),
            Column('userid', String),
            Column('wa', String),
            Column('balance', Integer),
            Column('reward', Integer),
            Column('tx', String),
            Column('date', Date),
            Column('reward_round', Integer)
        )

        # Define waMap table
        self.waMap = Table(
            'waMap', metadata,
            Column('ID', Integer, primary_key=True, autoincrement=True),
            Column('platform', String),
            Column('userID', String),
            Column('username', String),
            Column('wa', String),
            Column('date', Date)
        )

        # Create tables
        metadata.create_all(self.engine)

    def get_last_winner(self):
        with Session(self.engine) as session:
            query = select(self.rewards.c.userid)\
                .order_by(desc(self.rewards.c.date), desc(self.rewards.c.ID))\
                .limit(1)
            result = session.execute(query).first()
            return result[0] if result else None

    def get_latest_wa(self, identifier, value):
        with Session(self.engine) as session:
            column = self.waMap.c.userID if identifier == 'userID' else self.waMap.c.username
            query = select(self.waMap.c.wa)\
                .where(column == value)\
                .order_by(desc(self.waMap.c.ID))\
                .limit(1)
            result = session.execute(query).first()
            return result[0] if result else None

    def get_next_reward_round(self):
        with Session(self.engine) as session:
            query = select(self.rewards.c.reward_round)\
                .order_by(desc(self.rewards.c.reward_round))\
                .limit(1)
            result = session.execute(query).first()
            return (result[0] if result else 0) + 1

    def add_reward_entry(self, userid, reward_round=None, wa=None, balance=None, reward=None, tx=None):
        with Session(self.engine) as session:
            current_date = datetime.now().date()
            if not reward_round:
                reward_round = self.get_next_reward_round()

            if tx:
                # Update existing entry with transaction hash
                stmt = self.rewards.update()\
                    .where(self.rewards.c.reward_round == reward_round)\
                    .values(tx=tx)
                result = session.execute(stmt)
            else:
                # Check if entry exists
                query = select(self.rewards.c.ID)\
                    .where(self.rewards.c.reward_round == reward_round)
                existing = session.execute(query).first()

                if existing:
                    # Update existing entry
                    stmt = self.rewards.update()\
                        .where(self.rewards.c.reward_round == reward_round)\
                        .values(
                            wa=wa,
                            balance=balance,
                            reward=reward,
                            date=current_date
                        )
                    session.execute(stmt)
                else:
                    # Create new entry
                    stmt = self.rewards.insert().values(
                        userid=userid,
                        wa=wa,
                        balance=balance,
                        reward=reward,
                        tx=tx,
                        date=current_date,
                        reward_round=reward_round
                    )
                    result = session.execute(stmt)

            session.commit()
            return reward_round

    def add_pending_reward(self, userid):
        with Session(self.engine) as session:
            reward_round = self.get_next_reward_round()
            current_date = datetime.now().date()
            
            stmt = self.rewards.insert().values(
                userid=userid,
                date=current_date,
                reward_round=reward_round
            )
            session.execute(stmt)
            session.commit()
            return reward_round

    def get_pending_rewards(self):
        with Session(self.engine) as session:
            query = select(
                self.rewards.c.reward_round,
                self.rewards.c.userid,
                self.rewards.c.date
            ).where(
                self.rewards.c.wa.is_(None),
                self.rewards.c.tx.is_(None)
            ).order_by(
                self.rewards.c.date.asc(),
                self.rewards.c.reward_round.asc()
            )
            
            results = session.execute(query).fetchall()
            return results


class TelegramManager:
    def __init__(self):
        # User client credentials for getting messages
        self.api_id = os.getenv('TG_API_ID')
        self.api_hash = os.getenv('TG_API_HASH')
        self.group_id = int(os.getenv('GROUP_IDPROD'))
        
        # Bot credentials for sending messages
        self.bot_token = os.getenv('BOT_TOKEN')
        
        # Initialize both clients
        self.user_client = TelegramClient('user', self.api_id, self.api_hash)
        self.bot = Bot(token=self.bot_token)

    async def get_recent_messages(self, hours):
        """Get messages using user account"""
        await self.user_client.start()
        messages = []
        hours_ago = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        group = await self.user_client.get_entity(self.group_id)
        async for message in self.user_client.iter_messages(group):
            if message.date >= hours_ago:
                messages.append({
                    "username": message.sender.username if message.sender else None,
                    "userid": message.sender_id,
                    "text": message.text,
                    "date": message.date.isoformat()
                })
        
        await self.user_client.disconnect()
        return messages

    async def send_message(self, message):
        """Send message using bot account"""
        try:
            await self.bot.send_message(chat_id=self.group_id, text=message)
            return True
        except Exception as e:
            print(f"Failed to send message: {e}")
            return False


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
                f"📝 Please message @gMetisL2 to register your wallet address.\n"
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
