import uvloop
from pyrogram import Client, idle, __version__
from pyrogram.raw.all import layer
from mfinder import APP_ID, API_HASH, BOT_TOKEN
import os
from flask import Flask
from threading import Thread
import asyncio

uvloop.install()

async def main():
    # Initialize the bot
    plugins = dict(root="mfinder/plugins")
    app = Client(
        name="mfinder",
        api_id=APP_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        plugins=plugins,
    )
    async with app:
        me = await app.get_me()
        print(
            f"{me.first_name} - @{me.username} - Pyrogram v{__version__} (Layer {layer}) - Started..."
        )
        await idle()
        print(f"{me.first_name} - @{me.username} - Stopped !!!")

# Dummy port code for Render deployment
PORT = int(os.environ.get("PORT", 8080))  # Render sets the PORT environment variable
server = Flask(__name__)

@server.route("/")
def home():
    return "Bot is running!"

if __name__ == "__main__":
    # Run the Flask server in a separate thread
    Thread(target=lambda: server.run(host="0.0.0.0", port=PORT)).start()

    # Run the bot
    asyncio.run(main())
