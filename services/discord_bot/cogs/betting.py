import discord
from discord.ext import commands
from discord import app_commands
import uuid
from datetime import datetime, timedelta
from ui_components import MatchView

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

        view = MatchView(match_id, self.bot.db_pool, home_odds, draw_odds, away_odds, home_team, away_team)

        # ephemeral=True - only the user who invoked the command can see this message
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="dev_wynik", description="[DEV] Rozlicza mecz i wypłaca wygrane")
    @app_commands.describe(match_id="ID meczu do rozliczenia", outcome="Zwycięski typ (1, X, 2)")
    @app_commands.choices(outcome=[
        app_commands.Choice(name="1 (Wygrał Gospodarz)", value=1),
        app_commands.Choice(name="X (Remis)", value=0),
        app_commands.Choice(name="2 (Wygrał Gość)", value=2),
    ])
    async def dev_wynik(self, interaction: discord.Interaction, match_id: str, outcome: app_commands.Choice[int]):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("Brak uprawnień!", ephemeral=True)

        winning_prediction = outcome.value
        winning_team = match['home_team'] if winning_prediction == 1 else (match['away_team'] if winning_prediction == 2 else "Remis")

        async with self.bot.db_pool.acquire() as conn:
            # Open transcation
            async with conn.transaction():
                
                # Find the match and lock it for update to prevent race conditions
                match = await conn.fetchrow('SELECT status, home_team, away_team FROM matches WHERE match_id = $1 FOR UPDATE', match_id)
                
                if not match:
                    return await interaction.response.send_message("❌ Nie znaleziono meczu o takim ID w bazie.", ephemeral=True)
                if match['status'] == 'FINISHED':
                    return await interaction.response.send_message("❌ Ten mecz został już rozliczony!", ephemeral=True)

                # Change match status to FINISHED
                await conn.execute("UPDATE matches SET status = 'FINISHED' WHERE match_id = $1", match_id)

                # Select all bets for this match and lock them for update
                bets = await conn.fetch('SELECT * FROM bets WHERE match_id = $1 FOR UPDATE', match_id)

                bets_won = 0
                withdrawn_points = 0

                # Check each bet and update user points accordingly
                for bet in bets:
                    odds = float(bet['odds']) 
                    stake = int(bet['stake'])

                    if bet['status'] != 'PENDING':
                        continue # Skip already processed bets (shouldn't happen in this flow, but just in case)

                    if bet['prediction'] == winning_prediction:
                        # Player won
                        points_won = int(stake * odds)

                        # Update user's points balance
                        await conn.execute('UPDATE users SET points = points + $1 WHERE discord_id = $2', points_won, bet['discord_id'])
                        # Update bet status to WON
                        await conn.execute("UPDATE bets SET status = 'WON' WHERE id = $1", bet['id'])
                        
                        bets_won += 1
                        withdrawn_points += points_won
                    else:
                        # Player lost
                        pass
                        # Update status in bets table
                        await conn.execute("UPDATE bets SET status = 'LOST' WHERE id = $1", bet['id'])

        # Summary embed
        embed = discord.Embed(title="🏁 Mecz zakończony i rozliczony!", color=discord.Color.gold())
        embed.add_field(name="Spotkanie", value=f"{match['home_team']} vs {match['away_team']}", inline=False)
        embed.add_field(name="Wygrywający typ", value=winning_team, inline=False)
        embed.add_field(name="Statystyki", value=f"Wygrane kupony: **{bets_won}**\nRozdane punkty: 🪙 **{withdrawn_points}**", inline=False)

        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="moje_kupony", description="Wyświetla historię Twoich 10 ostatnich zakładów")
    async def moje_kupony(self, interaction: discord.Interaction):
        user_id = interaction.user.id

        async with self.bot.db_pool.acquire() as conn:
            bets = await conn.fetch('''
                SELECT b.*, m.home_team, m.away_team, m.start_time 
                FROM bets b 
                JOIN matches m ON b.match_id = m.match_id 
                WHERE b.discord_id = $1
                ORDER BY b.created_at DESC
                LIMIT 10
                ''', user_id)
            
        if not bets:
            return await interaction.response.send_message("Nie masz jeszcze żadnych kuponów. Zacznij obstawiać!", ephemeral=True)
        
        embed = discord.Embed(title="🎟️ Twoje kupony", color=discord.Color.purple())

        for bet in bets:
            # Determine the type of bet (home win, draw, away win)
            if bet['prediction'] == 1:
                typ = bet['home_team']
            elif bet['prediction'] == 2:
                typ = bet['away_team']
            else:
                typ = 'Remis'
            
            # Determine bet status and potential winnings
            if bet['status'] == 'WON':
                status_txt = "🟢 Wygrany"
                profit_txt = f"+{int(bet['stake'] * bet['odds'])}"
            elif bet['status'] == 'LOST':
                status_txt = "🔴 Przegrany"
                profit_txt = f"-{bet['stake']}"
            else:
                status_txt = "⏳ W toku"
                profit_txt = f"Do wygrania: {int(bet['stake'] * bet['odds'])}"

            embed.add_field(
                name=f"{bet['home_team']} vs {bet['away_team']}", 
                value=f"**Typ:** {typ}\n**Stawka:** {bet['stake']}\n**Kurs:** {bet['odds']}\n**Status:** {status_txt}\n**Zysk:** {profit_txt}", 
                inline=False
            )

        await interaction.response.send_message(embed=embed, ephemeral=True)

async def setup(bot):
    await bot.add_cog(BettingCog(bot))