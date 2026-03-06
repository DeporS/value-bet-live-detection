import discord
from discord.ext import commands
from discord import app_commands
import logging
from ui_components import MatchView

logger = logging.getLogger('discord_bot.daily_announcer')

class DailyAnnouncerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
    
    @app_commands.command(name="opublikuj_mecze", description="[ADMIN] Publikuje dzisiejsze mecze z bazy na obecnym kanale")
    async def opublikuj_mecze(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Brak uprawnień!", ephemeral=True)

        # Defer response to give bot more time to process (especially if there are many matches)
        await interaction.response.defer(ephemeral=True)

        async with self.bot.db_pool.acquire() as conn:
            # Get today's matches that haven't started yet (status = 'PRE_MATCH')
            matches = await conn.fetch('''
                SELECT match_id, home_team, away_team, home_odds, draw_odds, away_odds, start_time 
                FROM matches 
                WHERE status = 'PRE_MATCH' 
                AND DATE(start_time) = CURRENT_DATE
                AND start_time > NOW()
                ORDER BY start_time ASC
                LIMIT 25
            ''')

        if not matches:
            return await interaction.followup.send("📭 Nie ma dzisiaj w bazie żadnych nierozpoczętych meczów.")

        await interaction.followup.send(f"✅ Znaleziono {len(matches)} meczów. Rozpoczynam publikację...")
        
        channel = interaction.channel

        # Print each match with formatted time and odds
        for match in matches:
            # Change time format to readable
            unix_time = int(match['start_time'].timestamp())
            
            embed = discord.Embed(title=f"⚽ {match['home_team']} vs {match['away_team']}", color=discord.Color.blue())
            
            # <t:time:f> for readable date and <t:time:R> (fe. "za 2 godziny")
            embed.add_field(name="ID Meczu", value=f"`{match['match_id']}`", inline=False)
            embed.add_field(name="Rozpoczęcie", value=f"<t:{unix_time}:f> (<t:{unix_time}:R>)", inline=False)
            embed.add_field(name="Kursy", value=f"1: **{match['home_odds']}** | X: **{match['draw_odds']}** | 2: **{match['away_odds']}**", inline=False)
            
            # MatchView with buttons
            view = MatchView(
                match['match_id'], 
                self.bot.db_pool, 
                float(match['home_odds']), 
                float(match['draw_odds']), 
                float(match['away_odds']),
                match['home_team'],
                match['away_team']
            )
            
            await channel.send(embed=embed, view=view)

async def setup(bot):
    await bot.add_cog(DailyAnnouncerCog(bot))