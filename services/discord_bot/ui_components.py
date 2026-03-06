import discord
from discord import ui

class BetModal(ui.Modal):
    def __init__(self, match_id, team, prediction, odds, bot_pool):
        modal_title = f"Zakład: {team}"
        # Truncate title if it's too long for Discord's modal limit
        if len(modal_title) > 45:
            modal_title = modal_title[:42] + "..."

        super().__init__(title=modal_title)

        self.match_id = match_id
        self.team = team
        self.prediction = prediction
        self.odds = odds
        self.pool = bot_pool

        self.bet = ui.TextInput(
            label=f'Kurs: {self.odds}\nStawka: ',
            placeholder=f'Wygrana to: stawka x {self.odds}',
            min_length=1,
            max_length=7,
        )

        self.add_item(self.bet)

    async def on_submit(self, interaction: discord.Interaction):
        if not self.bet.value.isdigit():
            return await interaction.response.send_message("❌ Wpisz liczbę!", ephemeral=True)
        
        val = int(self.bet.value)

        if val <= 0:
            return await interaction.response.send_message("❌ Stawka musi być większa niż 0!", ephemeral=True)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                match = await conn.fetchrow('SELECT home_team, away_team, status, start_time FROM matches WHERE match_id = $1 FOR UPDATE', self.match_id)
                
                # Match existence and status checks
                if not match:
                    return await interaction.response.send_message("❌ Ten mecz już nie istnieje w bazie.", ephemeral=True)
                if match['status'] != 'PRE_MATCH':
                    return await interaction.response.send_message("❌ Zakłady na ten mecz są już zamknięte (Mecz trwa lub się zakończył).", ephemeral=True)

                # Check if user has enough points
                user = await conn.fetchrow('SELECT points FROM users WHERE discord_id = $1 FOR UPDATE', interaction.user.id)
                if not user or user['points'] < val:
                    return await interaction.response.send_message(f"❌ Brak środków! Posiadasz {user['points']} pkt.", ephemeral=True)

                # Deduct points and record the bet
                await conn.execute('UPDATE users SET points = points - $1 WHERE discord_id = $2', val, interaction.user.id)
                await conn.execute('INSERT INTO bets (discord_id, match_id, prediction, stake, odds) VALUES ($1, $2, $3, $4, $5)',
                                   interaction.user.id, self.match_id, self.prediction, val, self.odds)

        await interaction.response.send_message(f"✅ Postawiono {val} pkt na {self.team} (Kurs: {self.odds}) - do wygrania {round(val * self.odds)}", ephemeral=True)

class MatchView(ui.View):
    def __init__(self, match_id, bot_pool, h_odds, d_odds, a_odds, h_team, a_team):
        super().__init__(timeout=None)
        self.match_id = match_id
        self.pool = bot_pool
        self.odds = {1: h_odds, 0: d_odds, 2: a_odds}
        self.teams = {1: h_team, 0: "Remis", 2: a_team}

    @ui.button(label="1", style=discord.ButtonStyle.green)
    async def btn_1(self, interaction: discord.Interaction, btn: ui.Button):
        await interaction.response.send_modal(BetModal(self.match_id, self.teams[1], 1, self.odds[1], self.pool))

    @ui.button(label="X", style=discord.ButtonStyle.grey)
    async def btn_x(self, interaction: discord.Interaction, btn: ui.Button):
        await interaction.response.send_modal(BetModal(self.match_id, self.teams[0], 0, self.odds[0], self.pool))

    @ui.button(label="2", style=discord.ButtonStyle.red)
    async def btn_2(self, interaction: discord.Interaction, btn: ui.Button):
        await interaction.response.send_modal(BetModal(self.match_id, self.teams[2], 2, self.odds[2], self.pool))

    @ui.button(label="Śledź", style=discord.ButtonStyle.secondary, emoji="🔔", row=1)
    async def btn_track(self, interaction: discord.Interaction, button: ui.Button):
        user_id = interaction.user.id
        
        async with self.pool.acquire() as conn:
            # Check if user is registered in the system
            user_exists = await conn.fetchrow('SELECT 1 FROM users WHERE discord_id = $1', user_id)
            if not user_exists:
                return await interaction.response.send_message("❌ Zanim zaczniesz śledzić mecze, załóż profil wpisując komendę `/profil`!", ephemeral=True)

            existing = await conn.fetchrow('SELECT 1 FROM tracked_matches WHERE discord_id = $1 AND match_id = $2', user_id, self.match_id)
            
            if existing:
                await conn.execute('DELETE FROM tracked_matches WHERE discord_id = $1 AND match_id = $2', user_id, self.match_id)
                await interaction.response.send_message(f"🔕 Wyłączono powiadomienia dla meczu {self.teams[1]} vs {self.teams[2]}.", ephemeral=True)
            else:
                await conn.execute('INSERT INTO tracked_matches (discord_id, match_id) VALUES ($1, $2)', user_id, self.match_id)
                await interaction.response.send_message(f"🔔 Będziesz otrzymywać powiadomienia o meczu {self.teams[1]} vs {self.teams[2]}!", ephemeral=True)