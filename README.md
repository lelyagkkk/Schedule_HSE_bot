Telegram Bot for Booking Participants via Excel

This bot allows users to choose an available time slot (date + time), enter their full name and phone number, and confirm their booking.
The booking is then saved to an Excel file, either locally or on Yandex Disk.

Features

Shows a list of available experiments on /start

Loads experiments from experiments.json, so new experiments can be added easily

Includes an Other experiments button for switching between experiments

Displays experiment-specific terms and conditions before booking

Prevents users from selecting slots until they confirm the terms

After confirmation, shows a menu with Book a slot and Reschedule booking

Displays available slots in 2-week windows

If there are no slots in the ближайшие 2 weeks, automatically jumps to the next window that contains available slots

Allows users to browse slot windows using Back and Next

In rescheduling mode, shows the current booking, available new slots, and a Cancel booking button

Removes booked slots from availability immediately

After a slot is selected, asks for full name and phone number

Saves the following information to Excel:

Telegram user (@username, or id:<telegram_id> if no username is set)

full name

phone number

booking timestamp

Excel file format

The Excel file is created automatically on first launch (default: slots.xlsx), using the Slots sheet with the following columns:

Date — slot date (for example, 10.03.2026)

Time — slot time (for example, 14:30)

Telegram — filled in by the bot

FullName — filled in by the bot

Phone — filled in by the bot

BookedAt — filled in by the bot

The bot also supports your custom column format:



The column order is detected automatically based on the headers.

Supported date/time formats include values such as:

03.03.26 (Tuesday)

11:00 - 16:00

To add slots manually, fill in only the date and time columns (Date/Time or День/Время) in new rows.

If the file is open in Excel in edit mode, the bot may temporarily fail to write data.

When a slot is booked, the corresponding row (date / time / full name / phone / Telegram) is highlighted in light green.

Storage modes

The bot supports two storage modes:

local — the Excel file is stored next to the bot (EXCEL_PATH)

yadisk — the Excel file is stored on Yandex Disk (YADISK_PATH)

Experiments configuration

The experiments.json file contains the list of experiments shown to users when they start the bot.

Example:

{
  "experiments": [
    {
      "id": "fnirs_tdcs",
      "title": "Decision-Making Processes Study (fNIRS + tDCS)",
      "default_terms_text": "Terms text...",
      "scientist_id": "@ivanov_scientist",
      "extra_params": {
        "max_weekly_hours": 16
      },
      "storage_mode": "yadisk",
      "yadisk_path": "disk:/Participants tDCS.xlsx"
    },
    {
      "id": "demo_local",
      "title": "Example of a Second Experiment",
      "default_terms_text": "Terms text for the second experiment...",
      "scientist_id": "@petrova_scientist",
      "storage_mode": "local",
      "excel_path": "slots_demo.xlsx"
    }
  ]
}
Fields

id — technical identifier (latin letters / numbers / _)

title — experiment title shown to users

default_terms_text — terms and conditions for the experiment (required)

scientist_id — Telegram @username of the experimenter, shown in error messages (required)

terms_text — alias for default_terms_text

terms — deprecated alias for default_terms_text, supported for backward compatibility

max_weekly_hours — weekly limit for the total duration of slots in this experiment, in hours (optional)

default_slot_duration_hours — default slot duration if the time interval cannot be parsed from the Excel cell (optional)

extra_params — additional experiment parameters; supported keys:

max_weekly_hours

default_slot_duration_hours

slot_mode

working_hours

excluded_days

slot_duration_hours

slot_step_minutes

available_days_ahead

available_days_ahead — how many days ahead slots are visible to participants; also used as the LabShake synchronization horizon

storage_mode — local or yadisk

excel_path — file path for local mode

yadisk_path — file path or link for Yandex Disk mode

yadisk_token_env — optional name of the environment variable containing the Yandex Disk token

Installation

Install Python 3.10 or newer.

Install dependencies:

pip install -r requirements.txt

Create a .env file based on the example:

copy .env.example .env

Configure .env.

Local storage mode
BOT_TOKEN=<your BotFather token>
STORAGE_MODE=local
EXCEL_PATH=slots.xlsx
EXPERIMENT_TERMS=Please read the experiment terms before booking.\nIf you have read the terms and meet the requirements, press the button below.
EXPERIMENTS_FILE=experiments.json
DEFAULT_EXPERIMENT_TITLE=Decision-Making Processes Study (fNIRS + tDCS)
Yandex Disk mode
BOT_TOKEN=<your BotFather token>
STORAGE_MODE=yadisk
YADISK_TOKEN=<your Yandex Disk OAuth token>
YADISK_PATH=disk:/Participants tDCS.xlsx
EXPERIMENT_TERMS=Please read the experiment terms before booking.\nIf you have read the terms and meet the requirements, press the button below.
EXPERIMENTS_FILE=experiments.json
DEFAULT_EXPERIMENT_TITLE=Decision-Making Processes Study (fNIRS + tDCS)

You can also use a link like https://disk.360.yandex.com/edit/disk/... in YADISK_PATH; the bot will try to extract the file path automatically.

Important: an edit link alone is not enough for writing access. The bot also needs a YADISK_TOKEN with read/write permissions.

How to get a YADISK_TOKEN

Create an OAuth application in Yandex:
https://oauth.yandex.ru/client/new

Enable API access and grant Yandex Disk permissions.

Set the Redirect URI to:
https://oauth.yandex.ru/verification_code

After creating the application, copy the ClientID.

Open this URL, replacing YOUR_CLIENT_ID with your actual client ID:

https://oauth.yandex.ru/authorize?response_type=token&client_id=YOUR_CLIENT_ID

Grant access. After redirect, the address bar will contain something like:

#access_token=...&token_type=bearer&expires_in=...

Copy the access_token value into .env as YADISK_TOKEN.

Checking Yandex Disk access

Before starting the bot, you can test the storage connection:

python bot.py --check-storage

If everything is configured correctly, the command will return:

OK: ...
Running the bot
python bot.py
Bot commands

/start — show the list of experiments

/experiments — show the list of experiments

/book — start a new booking

/move — reschedule an existing booking

/menu — show the action menu

/cancel — cancel the current action (for example, entering full name or phone number)

LabShake auto-login

If LABSHAKE_COOKIE expires, the bot can log in automatically and refresh the session cookies.

Add the following to .env:

LABSHAKE_AUTO_LOGIN=1
LABSHAKE_LOGIN_EMAIL=your_email
LABSHAKE_LOGIN_PASSWORD=your_password
LABSHAKE_LOGIN_URL=https://labshake.com/sign-in
LABSHAKE_HEADLESS=1
LABSHAKE_LOGIN_TIMEOUT_SEC=45

Install the Playwright browser once:

python -m playwright install chromium

If LabShake is protected by Cloudflare, also set:

LABSHAKE_BROWSER_CHANNEL=chrome