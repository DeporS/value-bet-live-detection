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
        # Weryfikacja uprawnień
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Brak uprawnień!", ephemeral=True)

        # Mówimy Discordowi: "Daj mi chwilę, muszę pomyśleć", żeby zapobiec błędowi przekroczenia czasu (3 sekundy)
        await interaction.response.defer(ephemeral=True)

        async with self.bot.db_pool.acquire() as conn:
            # Pobieramy mecze, które są na dzisiaj i jeszcze się nie rozpoczęły
            matches = await conn.fetch('''
                SELECT match_id, home_team, away_team, home_odds, draw_odds, away_odds, start_time 
                FROM matches 
                WHERE status = 'PRE_MATCH' 
                AND DATE(start_time) = CURRENT_DATE
                ORDER BY start_time ASC
            ''')

        if not matches:
            return await interaction.followup.send("📭 Nie ma dzisiaj w bazie żadnych nierozpoczętych meczów.")

        await interaction.followup.send(f"✅ Znaleziono {len(matches)} meczów. Rozpoczynam publikację...")
        
        channel = interaction.channel

        # Publikujemy każdy mecz jako osobną wiadomość z przyciskami
        for match in matches:
            # Zamiana czasu z bazy na format czytelny dla Discorda
            unix_time = int(match['start_time'].timestamp())
            
            embed = discord.Embed(title=f"⚽ {match['home_team']} vs {match['away_team']}", color=discord.Color.blue())
            
            # Używamy <t:czas:f> dla dokładnej daty i <t:czas:R> dla odliczania (np. "za 2 godziny")
            embed.add_field(name="Rozpoczęcie", value=f"<t:{unix_time}:f> (<t:{unix_time}:R>)", inline=False)
            embed.add_field(name="Kursy", value=f"1: **{match['home_odds']}** | X: **{match['draw_odds']}** | 2: **{match['away_odds']}**", inline=False)
            
            # Podpinamy interfejs z przyciskami (MatchView z ui_components.py)
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