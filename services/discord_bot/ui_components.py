import discord
from discord import ui

class BetModal(ui.Modal, title='Postaw Zakład'):
    bet = ui.TextInput(
        label='Ile punktów stawiasz?',
        placeholder='np. 100',
        min_length=1,
        max_length=7
    )

    def __init__(self, match_id, prediction, odds, bot_pool):
        super().__init__()
        self.match_id = match_id
        self.prediction = prediction
        self.odds = odds
        self.pool = bot_pool

    async def on_submit(self, interaction: discord.Interaction):
        if not self.bet.value.isdigit():
            return await interaction.response.send_message("❌ Wpisz liczbę!", ephemeral=True)
        
        val = int(self.bet.value)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                user = await conn.fetchrow('SELECT points FROM users WHERE discord_id = $1 FOR UPDATE', interaction.user.id)
                if not user or user['points'] < val:
                    return await interaction.response.send_message(f"❌ Brak środków! Posiadasz {user['points']} pkt.", ephemeral=True)

                await conn.execute('UPDATE users SET points = points - $1 WHERE discord_id = $2', val, interaction.user.id)
                await conn.execute('INSERT INTO bets (discord_id, match_id, prediction, stake, odds) VALUES ($1, $2, $3, $4, $5)',
                                   interaction.user.id, self.match_id, self.prediction, val, self.odds)

        await interaction.response.send_message(f"✅ Postawiono {val} pkt na typ {self.prediction} (Kurs: {self.odds})")

class MatchView(ui.View):
    def __init__(self, match_id, bot_pool, h_odds, d_odds, a_odds):
        super().__init__(timeout=None)
        self.match_id = match_id
        self.pool = bot_pool
        self.odds = {1: h_odds, 0: d_odds, 2: a_odds}

    @ui.button(label="1", style=discord.ButtonStyle.green)
    async def btn_1(self, interaction: discord.Interaction, btn: ui.Button):
        await interaction.response.send_modal(BetModal(self.match_id, 1, self.odds[1], self.pool))

    @ui.button(label="X", style=discord.ButtonStyle.grey)
    async def btn_x(self, interaction: discord.Interaction, btn: ui.Button):
        await interaction.response.send_modal(BetModal(self.match_id, 0, self.odds[0], self.pool))

    @ui.button(label="2", style=discord.ButtonStyle.red)
    async def btn_2(self, interaction: discord.Interaction, btn: ui.Button):
        await interaction.response.send_modal(BetModal(self.match_id, 2, self.odds[2], self.pool))