import discord
from discord.ext import commands
from discord import app_commands

class ProfileCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name='profil', description="Sprawdź swój profil i stan konta")
    async def profil(self, interaction: discord.Interaction):
        """Command to register and check user profile and points balance"""
        user_id = interaction.user.id
        username = interaction.user.name

        # Open a connection from the pool and execute database operations
        async with self.bot.db_pool.acquire() as conn:
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
        embed.add_field(name="Gracz", value=username, inline=False)
        embed.add_field(name="Stan konta", value=f"🪙 **{points}** pkt", inline=False)

        await interaction.response.send_message(content=msg, embed=embed, ephemeral=True)

async def setup(bot):
    await bot.add_cog(ProfileCog(bot))