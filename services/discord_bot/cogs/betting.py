import discord
from discord.ext import commands
from discord import app_commands
import uuid
from datetime import datetime, timedelta

class BettingCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="dev_mecz", description="[DEV] Generuje testowy mecz w bazie")
    async def dev_mecz(self, interaction: discord.Interaction):
        """Hidden command for developers to create a test match in the database"""
        
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("Brak uprawnień!", ephemeral=True)

        match_id = str(uuid.uuid4())[:8] # short id
        home_team = "Real Madryt"
        away_team = "FC Barcelona"
        home_odds = 2.80
        draw_odds = 3.40
        away_odds = 2.50
        
        # Symulate match starting in 2 hours
        start_time = datetime.now() + timedelta(hours=2)

        async with self.bot.db_pool.acquire() as conn:
            await conn.execute(
                '''
                INSERT INTO matches 
                (match_id, home_team, away_team, home_odds, draw_odds, away_odds, start_time) 
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ''',
                match_id, home_team, away_team, home_odds, draw_odds, away_odds, start_time
            )

        embed = discord.Embed(title="🛠️ [DEV] Utworzono mecz testowy", color=discord.Color.green())
        embed.add_field(name="ID Meczu", value=f"`{match_id}`", inline=False)
        embed.add_field(name="Spotkanie", value=f"{home_team} vs {away_team}", inline=False)
        embed.add_field(name="Kursy", value=f"1: **{home_odds}** | X: **{draw_odds}** | 2: **{away_odds}**", inline=False)
        
        # ephemeral=True - only the user who invoked the command can see this message
        await interaction.response.send_message(embed=embed, ephemeral=True)

async def setup(bot):
    await bot.add_cog(BettingCog(bot))