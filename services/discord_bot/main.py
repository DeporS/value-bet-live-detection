import os
import discord
from discord.ext import commands
import logging
from database import get_db_pool, init_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('discord_bot.main')

# Bot class
class TyperBot(commands.Bot):
    def __init__(self):
        # Define intents that bot downloads from Discord
        intents = discord.Intents.default()
        super().__init__(command_prefix='!', intents=intents)
        self.db_pool = None
    
    async def setup_hook(self):
        """This function is called once when the bot starts"""
        logger.info("Setting up database connection pool...")
        # Create database connection pool
        self.db_pool = await get_db_pool() 

        # Initialize database (create tables if they don't exist)
        await init_db(self.db_pool)

        # Synchronize command tree with Discord (register slash commands)
        await self.tree.sync()
        logger.info("Bot setup complete. Command tree synced.")
    
    async def close(self):
        """Gracefully close database pool when bot is shutting down"""
        if self.db_pool:
            await self.db_pool.close()
        await super().close()

bot = TyperBot()

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    
@bot.tree.command(name='profil', description="Sprawdź swój profil i stan konta")
async def profil(interaction: discord.Interaction):
    """Command to register and check user profile and points balance"""
    user_id = interaction.user.id
    username = interaction.user.name

    # Open a connection from the pool and execute database operations
    async with bot.db_pool.acquire() as conn:
        # Check if user already exists (Using $1 for parameterized query to prevent SQL injection)
        record = await conn.fetchrow('SELECT points FROM users WHERE discord_id = $1', user_id)

        if record is None:
            # User does not exist, create new record with default points
            await conn.execute(
                'INSERT INTO users (discord_id, username, points) VALUES ($1, $2, 1000)',
                user_id, username
            )
            points = 1000
            msg = f"Witaj na pokładzie **Odds Snipers**, **{username}**! 🚀 Na start dostajesz 1000 punktów. Nie przewal ich za szybko!"
        else:
            # Player already exists, fetch current points
            points = record['points']
            msg = f"Witaj ponownie, **{username}**!"
        
    # Create an embed message to display user profile and points
    embed = discord.Embed(title="👤 Profil Gracza", color=discord.Color.blue())
    embed.add_field(name="Gracz", value=username, inline=True)
    embed.add_field(name="Stan konta", value=f"🪙 **{points}** pkt", inline=True)

    await interaction.response.send_message(content=msg, embed=embed)

if __name__ == '__main__':
    token = os.getenv('DISCORD_TOKEN')
    if not token:
        logger.error("No DISCORD_TOKEN found in environment variables.")
    else:
        bot.run(token)