import os
import json
import asyncio
import requests
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from web3 import Web3
import openai
import tweepy

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, select, desc, update, insert
from sqlalchemy.orm import Session
from tenacity import retry, stop_after_attempt, wait_exponential


# global constants
WINDOW_IN_H = 24
TOTAL_REWARD = 10
AMOUNT_HELD = 100
BOT_USERNAME = os.getenv('BOT_USERNAME')
EXCLUDED_USERS = ["user1", "user2"]  # Usernames instead of IDs

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Import OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

class DatabaseManager:
    def __init__(self):
        server = os.getenv('AZURE_SQL_SERVER')
        database = os.getenv('AZURE_SQL_DATABASE')
        username = os.getenv('AZURE_SQL_USERNAME')
        password = os.getenv('AZURE_SQL_PASSWORD')
        
        self.connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server'
        
        # Add retry logic and longer timeout
        self.engine = create_engine(
            self.connection_string,
            connect_args={
                'timeout': 300,  # Increase timeout to 5 minutes
                'retry_with_backoff': True,
                'backoff_factor': 2
            },
            pool_size=5,
            max_overflow=10
        )
        self.setup_database()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def setup_database(self):
        # Create tables using SQLAlchemy
        metadata = MetaData()
        
        # Define xrewards table
        self.rewards = Table(
            'xrewards', metadata,
            Column('ID', Integer, primary_key=True, autoincrement=True),
            Column('username', String),  # Using username instead of userid
            Column('post_id', String),
            Column('wa', String),
            Column('balance', Integer),
            Column('reward', Integer),
            Column('tx', String),
            Column('date', Date),
            Column('reward_round', Integer)
        )

        # Define xwaMap table
        self.waMap = Table(
            'xwaMap', metadata,
            Column('ID', Integer, primary_key=True, autoincrement=True),
            Column('platform', String),
            Column('username', String),  # Using username as identifier
            Column('wa', String),
            Column('date', Date)
        )

        # Create tables if they don't exist
        metadata.create_all(self.engine)

    def get_last_winner(self):
        with Session(self.engine) as session:
            query = select(self.rewards.c.username)\
                .order_by(desc(self.rewards.c.date), desc(self.rewards.c.ID))\
                .limit(1)
            result = session.execute(query).first()
            return result[0] if result else None

    def get_latest_wa(self, username):
        with Session(self.engine) as session:
            query = select(self.waMap.c.wa)\
                .where(self.waMap.c.username == username)\
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

    def add_reward_entry(self, username, post_id, reward_round=None, wa=None, balance=None, reward=None, tx=None):
        with Session(self.engine) as session:
            current_date = datetime.now().date()
            if not reward_round:
                reward_round = self.get_next_reward_round()
    
            try:
                if tx:
                    # Update existing entry with transaction hash
                    stmt = update(self.rewards).where(
                        self.rewards.c.reward_round == reward_round
                    ).values(tx=tx)
                    session.execute(stmt)
                else:
                    # Check if entry exists for this reward round
                    query = select(self.rewards.c.ID).where(
                        self.rewards.c.reward_round == reward_round
                    )
                    existing = session.execute(query).first()
    
                    if existing:
                        # Update existing entry
                        stmt = update(self.rewards).where(
                            self.rewards.c.reward_round == reward_round
                        ).values(
                            wa=wa,
                            balance=balance,
                            reward=reward,
                            date=current_date
                        )
                        session.execute(stmt)
                    else:
                        # Create new entry
                        stmt = insert(self.rewards).values(
                            username=username,
                            post_id=post_id,
                            wa=wa,
                            balance=balance,
                            reward=reward,
                            tx=tx,
                            date=current_date,
                            reward_round=reward_round
                        )
                        session.execute(stmt)
    
                session.commit()
                return reward_round
            except Exception as e:
                session.rollback()
                raise

    def add_pending_reward(self, username, post_id):
        with Session(self.engine) as session:
            reward_round = self.get_next_reward_round()
            current_date = datetime.now().date()
            
            stmt = insert(self.rewards).values(
                username=username,
                post_id=post_id,
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
                self.rewards.c.username,
                self.rewards.c.post_id,
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
        
# logger = logging.getLogger(__name__)


# class XManager:
#     def __init__(self):
#         # Set up Twitter API v1.1 authentication
#         auth = tweepy.OAuth1UserHandler(
#             consumer_key=os.getenv('TWITTER_CONSUMER_KEY'),
#             consumer_secret=os.getenv('TWITTER_CONSUMER_SECRET'),
#             access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
#             access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
#         )
#         self.client = tweepy.API(auth)
#         self.bot_username = os.getenv('BOT_USERNAME')
        
#     async def initialize(self):
#         # Verify credentials
#         try:
#             me = self.client.verify_credentials()
#             logger.info(f"Successfully signed in as @{me.screen_name}")
#         except Exception as e:
#             logger.error(f"Failed to sign in: {e}")
#             raise

#     async def get_recent_mentions(self, hours):
#         hours_ago = datetime.now(timezone.utc) - timedelta(hours=hours)
        
#         try:
#             # Get mentions using Twitter API v1.1
#             mentions = self.client.mentions_timeline(count=100)
            
#             if not mentions:
#                 logger.info("No mentions found")
#                 return []
                
#             # Log the raw response for debugging
#             logger.info(f"Found {len(mentions)} mentions")
            
#             recent_mentions = []
            
#             for tweet in mentions:
#                 tweet_time = tweet.created_at
                
#                 if tweet_time >= hours_ago:
#                     # Get user info
#                     author = self.client.get_user(id=tweet.author_id)
#                     tweet.author_username = author.screen_name
                    
#                     # Add public metrics
#                     tweet.likes = tweet.favorite_count
#                     tweet.retweet_counts = tweet.retweet_count
#                     tweet.reply_counts = tweet.reply_count
                    
#                     recent_mentions.append(tweet)
#                     logger.info(f"Found mention from @{tweet.author_username} at {tweet_time}")
#                 else:
#                     break
            
#             logger.info(f"Found {len(recent_mentions)} recent mentions")
#             return recent_mentions
            
#         except Exception as e:
#             logger.error(f"Error searching mentions: {e}")
#             return []

#     def get_most_engaging_post(self, posts, excluded_users):
#         if not posts:
#             return None, None
        
#         try:
#             # Sort posts by engagement (likes + retweets + replies)
#             sorted_posts = sorted(
#                 posts,
#                 key=lambda x: (
#                     getattr(x, 'likes', 0) + 
#                     getattr(x, 'retweet_counts', 0) + 
#                     getattr(x, 'reply_counts', 0)
#                 ),
#                 reverse=True
#             )
            
#             for post in sorted_posts:
#                 try:
#                     author = post.author_username
#                     if author not in excluded_users:
#                         logger.info(f"Selected winning post from @{author}")
#                         return post, author
#                 except Exception as e:
#                     logger.error(f"Error getting author from post: {e}")
#                     continue
            
#             logger.info("No eligible posts found after filtering")
#             return None, None
            
#         except Exception as e:
#             logger.error(f"Error in get_most_engaging_post: {e}")
#             return None, None

#     async def reply_to_tweet(self, tweet_id, message):
#         try:
#             # Reply to tweet using Twitter API v1.1
#             response = self.client.update_status(
#                 status=message,
#                 in_reply_to_status_id=tweet_id
#             )
            
#             if response:
#                 logger.info(f"Successfully replied to tweet {tweet_id}")
#                 return response.id
#             else:
#                 logger.error(f"Failed to reply to tweet {tweet_id} - no response")
#                 return None
                
#         except Exception as e:
#             logger.error(f"Failed to send tweet: {e}")
#             return None

import os
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from playwright.async_api import async_playwright
import re

import os
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from playwright.async_api import async_playwright
import re
import time

class XManager:
    def __init__(self):
        self.username = os.getenv('TWITTER_USERNAME')
        self.password = os.getenv('TWITTER_PASSWORD')
        self.bot_username = os.getenv('BOT_USERNAME')
        self.browser = None
        self.page = None
        self.context = None
        self.playwright = None
        self.logger = logging.getLogger(__name__)
        
    async def initialize(self):
        try:
            # Start playwright and browser
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=False,  # Set to True in production
                args=['--disable-blink-features=AutomationControlled']  # Help avoid detection
            )
            
            # Create a persistent context with storage state if it exists
            cookie_file = 'twitter_cookies.json'
            storage_state = cookie_file if os.path.exists(cookie_file) else None
            
            self.context = await self.browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
                storage_state=storage_state
            )
            
            # Enable additional logging
            self.context.set_default_timeout(60000)  # Increase timeout to 60 seconds
            
            self.page = await self.context.new_page()
            
            # Go to Twitter/X home page first, then to login
            await self.page.goto("https://twitter.com/", wait_until="domcontentloaded")
            await asyncio.sleep(3)
            
            # Check if already logged in by looking for home timeline
            is_logged_in = await self._check_if_logged_in()
            
            if not is_logged_in:
                self.logger.info("Not logged in. Starting login process...")
                await self._perform_login()
            else:
                self.logger.info("Already logged in.")
            
            # Save cookies for future sessions
            await self.context.storage_state(path=cookie_file)
            
            self.logger.info(f"Successfully initialized XManager")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize: {str(e)}")
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            raise
    
    async def _check_if_logged_in(self):
        try:
            # Check for elements that would indicate we're logged in
            home_timeline = await self.page.query_selector('div[data-testid="primaryColumn"]')
            profile_icon = await self.page.query_selector('div[data-testid="SideNav_AccountSwitcher_Button"]')
            
            return home_timeline is not None or profile_icon is not None
        except:
            return False
    
    async def _perform_login(self):
        try:
            # Look for login button on homepage
            login_button = await self.page.query_selector('a[data-testid="login"]')
            if login_button:
                await login_button.click()
            else:
                # Go directly to login page if button not found
                await self.page.goto("https://twitter.com/login", wait_until="domcontentloaded")
            
            await asyncio.sleep(3)
            
            # Take screenshot for debugging
            await self.page.screenshot(path="login_screen.png")
            
            # Twitter's login flow has multiple possible selectors
            # Try different username field selectors
            username_selectors = [
                'input[autocomplete="username"]',
                'input[name="text"]',
                'input[autocomplete="email"]',
                'input[data-testid="ocfEnterTextTextInput"]'
            ]
            
            username_input = None
            for selector in username_selectors:
                username_input = await self.page.query_selector(selector)
                if username_input:
                    self.logger.info(f"Found username field with selector: {selector}")
                    break
            
            if not username_input:
                self.logger.error("Could not find username input field")
                await self.page.screenshot(path="error_username_field.png")
                raise Exception("Username field not found")
            
            # Enter username
            await username_input.fill(self.username)
            await asyncio.sleep(1)
            
            # Find and click the Next button with various possible selectors
            next_button_selectors = [
                'div[role="button"]:has-text("Next")',
                'span:has-text("Next")',
                'div[data-testid="auth_next_button"]'
            ]
            
            next_clicked = False
            for selector in next_button_selectors:
                try:
                    next_button = await self.page.query_selector(selector)
                    if next_button:
                        await next_button.click()
                        next_clicked = True
                        break
                except:
                    continue
            
            if not next_clicked:
                # Try just pressing Enter
                await self.page.keyboard.press('Enter')
            
            await asyncio.sleep(3)
            
            # Handle the "unusual login activity" verification if it appears
            verify_box = await self.page.query_selector('div:has-text("Verify your identity")')
            if verify_box:
                self.logger.info("Identity verification required")
                # Handle the verification flow
                # This might include entering a phone number or email
                # depending on Twitter's verification method
                # You'll need to customize this based on the verification method
            
            # Wait for password field
            password_selectors = [
                'input[type="password"]',
                'input[name="password"]',
                'input[data-testid="ocfEnterTextPasswordInput"]'
            ]
            
            password_input = None
            for i in range(10):  # Try multiple times
                for selector in password_selectors:
                    password_input = await self.page.query_selector(selector)
                    if password_input:
                        self.logger.info(f"Found password field with selector: {selector}")
                        break
                if password_input:
                    break
                await asyncio.sleep(1)
            
            if not password_input:
                self.logger.error("Could not find password input field")
                await self.page.screenshot(path="error_password_field.png")
                raise Exception("Password field not found")
            
            # Enter password
            await password_input.fill(self.password)
            await asyncio.sleep(1)
            
            # Find and click login button
            login_button_selectors = [
                'div[role="button"]:has-text("Log in")',
                'span:has-text("Log in")',
                'div[data-testid="LoginForm_Login_Button"]'
            ]
            
            login_clicked = False
            for selector in login_button_selectors:
                try:
                    login_button = await self.page.query_selector(selector)
                    if login_button:
                        await login_button.click()
                        login_clicked = True
                        break
                except:
                    continue
            
            if not login_clicked:
                # Try just pressing Enter
                await self.page.keyboard.press('Enter')
            
            # Wait for home timeline to load
            await self.page.wait_for_selector('div[data-testid="primaryColumn"]', timeout=30000)
            self.logger.info(f"Login successful")
            
        except Exception as e:
            self.logger.error(f"Login process failed: {str(e)}")
            await self.page.screenshot(path="login_error.png")
            raise
    
    async def get_recent_mentions(self, hours):
        hours_ago = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        try:
            # Navigate to notifications/mentions
            await self.page.goto(f"https://twitter.com/notifications/mentions", wait_until="domcontentloaded")
            await asyncio.sleep(5)  # Give time for the page to fully load
            
            # Check if we're on the mentions page
            mentions_header = await self.page.query_selector('div[data-testid="primaryColumn"] h2:has-text("Mentions")')
            if not mentions_header:
                self.logger.warning("Could not find Mentions header, may not be on the correct page")
                await self.page.screenshot(path="mentions_page.png")
            
            # Wait for the mentions to load
            await self.page.wait_for_selector('section[role="region"]', timeout=30000)
            
            # Scroll to load more mentions
            last_height = await self.page.evaluate('document.body.scrollHeight')
            mentions = []
            scroll_attempts = 0
            max_scroll_attempts = 20  # Set a reasonable limit
            
            # Keep scrolling until we either reach mentions older than the time window or max scrolls
            while scroll_attempts < max_scroll_attempts:
                # Get all tweet elements
                tweet_elements = await self.page.query_selector_all('article[data-testid="tweet"]')
                
                self.logger.info(f"Found {len(tweet_elements)} tweet elements on page")
                
                # Process new tweets
                for element in tweet_elements:
                    # Get tweet ID
                    link_element = await element.query_selector('a[href*="/status/"]')
                    if not link_element:
                        continue
                        
                    href = await link_element.get_attribute('href')
                    match = re.search(r'/status/(\d+)', href)
                    if not match:
                        continue
                        
                    tweet_id = match.group(1)
                    
                    # Check if we already processed this tweet
                    if any(getattr(tweet, 'id', None) == tweet_id for tweet in mentions):
                        continue
                    
                    # Get timestamp
                    time_element = await element.query_selector('time')
                    if not time_element:
                        continue
                        
                    datetime_str = await time_element.get_attribute('datetime')
                    tweet_time = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                    
                    # Stop if the tweet is older than our time window
                    if tweet_time < hours_ago:
                        self.logger.info(f"Found tweet older than {hours} hours, stopping scroll")
                        break
                    
                    # Get author username
                    author_element = await element.query_selector('[data-testid="User-Name"] a')
                    if not author_element:
                        continue
                        
                    author_href = await author_element.get_attribute('href')
                    author_username = author_href.split('/')[-1]
                    
                    # Get metrics
                    like_count = 0
                    retweet_count = 0
                    reply_count = 0
                    
                    # Metrics are often shown as groups of elements
                    metrics_elements = await element.query_selector_all('[data-testid="reply"], [data-testid="retweet"], [data-testid="like"]')
                    
                    for metric_element in metrics_elements:
                        testid = await metric_element.get_attribute('data-testid')
                        metric_text = await metric_element.text_content()
                        
                        if testid == "reply":
                            reply_count = self._parse_count(metric_text)
                        elif testid == "retweet":
                            retweet_count = self._parse_count(metric_text)
                        elif testid == "like":
                            like_count = self._parse_count(metric_text)
                    
                    # Create a tweet object similar to Tweepy's format
                    tweet = type('Tweet', (), {
                        'id': tweet_id,
                        'created_at': tweet_time,
                        'author_id': None,  # We don't need this
                        'author_username': author_username,
                        'likes': like_count,
                        'retweet_counts': retweet_count,
                        'reply_counts': reply_count
                    })
                    
                    mentions.append(tweet)
                    self.logger.info(f"Found mention from @{author_username} at {tweet_time} with {like_count} likes, {retweet_count} retweets, {reply_count} replies")
                
                # Check if we have tweets older than our time window
                if any(getattr(tweet, 'created_at', None) < hours_ago for tweet in mentions):
                    break
                
                # Scroll down
                await self.page.evaluate('window.scrollBy(0, 1000)')
                await asyncio.sleep(3)  # Give time for content to load
                
                # Check if we've scrolled to the bottom
                new_height = await self.page.evaluate('document.body.scrollHeight')
                if new_height == last_height:
                    scroll_attempts += 1
                    if scroll_attempts >= 3:  # If we can't scroll further after 3 attempts
                        break
                else:
                    scroll_attempts = 0  # Reset counter if we successfully scrolled
                    
                last_height = new_height
            
            # Filter out mentions older than our time window
            recent_mentions = [tweet for tweet in mentions if getattr(tweet, 'created_at', datetime.now()) >= hours_ago]
            self.logger.info(f"Found {len(recent_mentions)} recent mentions within {hours} hour window")
            return recent_mentions
            
        except Exception as e:
            self.logger.error(f"Error searching mentions: {str(e)}")
            await self.page.screenshot(path="mentions_error.png")
            return []

    def _parse_count(self, text):
        # Extract numbers from text like "5", "5.2K", etc.
        if not text or text.strip() == "":
            return 0
            
        try:
            text = text.strip()
            if 'K' in text:
                return int(float(text.replace('K', '')) * 1000)
            elif 'M' in text:
                return int(float(text.replace('M', '')) * 1000000)
            else:
                # Extract just the number
                match = re.search(r'\d+', text)
                if match:
                    return int(match.group())
                return 0
        except:
            return 0

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
                    author = post.author_username
                    if author not in excluded_users:
                        self.logger.info(f"Selected winning post from @{author}")
                        return post, author
                except Exception as e:
                    self.logger.error(f"Error getting author from post: {e}")
                    continue
            
            self.logger.info("No eligible posts found after filtering")
            return None, None
            
        except Exception as e:
            self.logger.error(f"Error in get_most_engaging_post: {e}")
            return None, None

    async def reply_to_tweet(self, tweet_id, message):
        try:
            # Navigate to the tweet
            await self.page.goto(f"https://twitter.com/i/status/{tweet_id}", wait_until="domcontentloaded")
            await asyncio.sleep(3)  # Wait for the page to fully load
            
            # Take screenshot for debugging
            await self.page.screenshot(path=f"tweet_{tweet_id}.png")
            
            # Click reply button
            reply_btn = await self.page.query_selector('div[data-testid="reply"]')
            if not reply_btn:
                self.logger.error("Could not find reply button")
                return None
                
            await reply_btn.click()
            await asyncio.sleep(2)
            
            # Wait for the reply textbox
            reply_box_selectors = [
                'div[data-testid="tweetTextarea_0"]',
                'div[role="textbox"][data-testid="tweetTextarea_0"]'
            ]
            
            reply_box = None
            for selector in reply_box_selectors:
                reply_box = await self.page.query_selector(selector)
                if reply_box:
                    break
                    
            if not reply_box:
                self.logger.error("Could not find reply textbox")
                await self.page.screenshot(path=f"reply_error_{tweet_id}.png")
                return None
                
            # Fill in the reply
            await reply_box.fill(message)
            await asyncio.sleep(1)
            
            # Click the tweet/reply button
            tweet_btn_selectors = [
                'div[data-testid="tweetButton"]',
                'div[role="button"][data-testid="tweetButton"]'
            ]
            
            tweet_btn = None
            for selector in tweet_btn_selectors:
                tweet_btn = await self.page.query_selector(selector)
                if tweet_btn:
                    break
                    
            if not tweet_btn:
                self.logger.error("Could not find tweet button")
                await self.page.screenshot(path=f"tweet_btn_error_{tweet_id}.png")
                return None
                
            # Check if the button is disabled
            is_disabled = await tweet_btn.get_attribute('aria-disabled') == 'true'
            if is_disabled:
                self.logger.error("Tweet button is disabled")
                await self.page.screenshot(path=f"tweet_btn_disabled_{tweet_id}.png")
                return None
                
            await tweet_btn.click()
            
            # Wait for the tweet to be sent (look for a success indicator)
            await asyncio.sleep(5)  # Give it time to send
            
            self.logger.info(f"Successfully replied to tweet {tweet_id}")
            
            # Try to get the reply tweet ID
            # This is difficult as Twitter doesn't clearly indicate which is our reply
            # For now, we'll just return the original tweet ID
            return tweet_id
                
        except Exception as e:
            self.logger.error(f"Failed to send tweet: {str(e)}")
            await self.page.screenshot(path=f"reply_exception_{tweet_id}.png")
            return None
            
    async def cleanup(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            self.logger.info("XManager resources cleaned up")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

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
            'gasPrice': self.web3.to_wei('3', 'gwei'),
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
        
        # Construct the message for OpenAI 
        message = f"🎯 Reward Round #{reward_round} - Retroactive Payout 🎯\n\n" \
                    f"✨ Congratulations @{username} \n" \
                    f"🎁 You've received {reward_amount} gMetis for writing the most active post! \n" \
                    f"📅 Original win date: {date}\n" \
                    f"🔗 Transaction: https://andromeda-explorer.metis.io/tx/0x{tx_hash}\n\n" \
                    f"🚀 Better late than never! Keep engaging! 🚀"

        # Call OpenAI API
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a professional social media writer. Rephrase the following crypto reward announcement message while keeping all the information, emojis and formatting. Make it engaging but professional."},
                {"role": "user", "content": message}
            ]
        )

        # Get rephrased message
        msg = response.choices[0].message.content.strip()

        # Log the rephrased message
        logger.info(f"Rephrased message: {msg}")

        # Reply to the tweet
        await x_manager.reply_to_tweet(post_id, msg)


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
            
            message = f"🎯 Reward Round #{reward_round}\n\n" \
                    f"🎉 Congratulations @{winner_username}! 🎉\n" \
                    f"✨ Your post has been selected for a reward! \n" \
                    f"📝 Please DM @{BOT_USERNAME} to register your wallet address.\n" \
                    f"💫 Your reward will be processed automatically once registered!"

            # Call OpenAI API
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a professional social media writer. Rephrase the following crypto reward announcement message while keeping all the information, emojis and formatting. Make it engaging but professional."},
                    {"role": "user", "content": message}
                ]
            )

            # Get rephrased message
            msg = response.choices[0].message.content.strip()

            # Log the rephrased message
            logger.info(f"Rephrased message: {msg}")
            
            await x_manager.reply_to_tweet(winning_post.id, msg)
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

            message = f"🎯 Reward Round #{reward_round}\n\n"\
                    f"🎉 Congratulations @{winner_username}! 🎉\n"\
                    f"✨ Thank you for engaging with us! \n"\
                    f"🎁 You've received {reward_amount} gMetis! \n"\
                    f"🔗 Transaction: https://andromeda-explorer.metis.io/tx/0x{tx_hash}\n\n"\
                    f"🚀 Keep engaging and stay awesome! 🚀"

            # Call OpenAI API
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a professional social media writer. Rephrase the following crypto reward announcement message while keeping all the information, emojis and formatting. Make it engaging but professional."},
                    {"role": "user", "content": message}
                ]
            )

            # Get rephrased message
            msg = response.choices[0].message.content.strip()

            # Log the rephrased message
            logger.info(f"Rephrased message: {msg}")

            await x_manager.reply_to_tweet(winning_post.id, msg)
        
        # Process any pending rewards
        await process_pending_rewards(db_manager, x_manager, web3_manager)
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())