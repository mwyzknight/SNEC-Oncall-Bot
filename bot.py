import os
import re
import pandas as pd
from datetime import datetime
import pytz
from dateparser.search import search_dates
from flask import Flask
from rapidfuzz import process, fuzz
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import threading
import time
import nest_asyncio

nest_asyncio.apply()

# =====================
# On Call Google Sheet 
# =====================

sheet_url = "https://docs.google.com/spreadsheets/d/1mZx6j7hEuJYeuYM0i79XUQ1gRJTg1I8cTnIgIGBHDyI/edit?gid=43119476#gid=43119476"
sheet_id = sheet_url.split("/d/")[1].split("/")[0]
tabs = ["MO_Reg", "Fellows", "Consultants"]

df = None

def fetch_sheet():
    global df
    dfs = []
    for tab in tabs:
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={tab}"
        df_tab = pd.read_csv(csv_url)
        df_tab.columns = df_tab.columns.str.strip()
        dfs.append(df_tab)

    base = dfs[0][["Date", "Day"]].copy()
    for df_extra in dfs:
        extra_cols = [c for c in df_extra.columns if c not in ["Date", "Day"]]
        base = pd.concat([base, df_extra[extra_cols]], axis=1)

    df = base
    df["Date"] = pd.to_datetime(df["Date"])
    print(f"Google Sheet refreshed at {datetime.now()}")

# Initial fetch
fetch_sheet()

# Background thread: refresh every 30mins
def periodic_refresh(interval_minutes=15):
    while True:
        time.sleep(interval_minutes * 60)
        fetch_sheet()

threading.Thread(target=periodic_refresh, daemon=True).start()

# =====================
# Phone Number Google Sheet 
# =====================

CONTACTS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1gKHurth2CRKufDoLI0WI_04XzZq2jZsw8tsmvV8kYUk/edit#gid=601029071"
CONTACTS_SHEET_ID = CONTACTS_SHEET_URL.split("/d/")[1].split("/")[0]
CONTACT_TABS = ["RPs_Residents_MOs", "AC_Reg", "SC_C", "Others"]

contacts_df = None

def fetch_contacts_sheet():
    global contacts_df
    all_dfs = []

    for tab in CONTACT_TABS:
        csv_url = f"https://docs.google.com/spreadsheets/d/{CONTACTS_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={tab}"
        df_tab = pd.read_csv(csv_url, keep_default_na=False)
        df_tab.columns = [c.strip() for c in df_tab.columns]

        df_tab = df_tab.rename(columns={
            "NAME": "NAME",
            "HANDPHONE": "HANDPHONE",
            "CO. HANDPHONE": "CO_HANDPHONE"
        })
        df_tab = df_tab_tab[df_tab["NAME"].notna() & df_tab["NAME"].str.strip() != ""]

        for col in ["HANDPHONE", "CO_HANDPHONE"]:
            if col not in df_tab.columns:
                df_tab[col] = ""

        def pick_phone(row):
            hand = row.get("HANDPHONE", "")
            co = row.get("CO_HANDPHONE", "")
            if hand and hand != "--":
                return str(hand)
            elif co and co != "--":
                return str(co)
            else:
                return "Not available"

        df_tab["PHONE_FINAL"] = df_tab.apply(pick_phone, axis=1)
        all_dfs.append(df_tab[["NAME", "PHONE_FINAL"]])

    contacts_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["NAME"], keep="first")
    contacts_df['NAME'] = contacts_df['NAME'].apply(lambda x: re.sub(r"\(.*?\)", "", x).strip())
    print("Contacts sheet refreshed ✅")

# Initial fetch + background refresh every 14 days
fetch_contacts_sheet()
def fortnight_refresh(interval_days=14):
    while True:
        time.sleep(interval_days * 86400)
        fetch_contacts_sheet()

threading.Thread(target=fortnight_refresh, daemon=True).start()

# =====================
# Google Collab Functions
# =====================

# =====================
# MO on call Function
# =====================
def MO_on_call(date, location):
    """
    Retrieve on-call info for a given date and location.
    location can be 'SNEC', 'CGH', 'KKH'
    """
    # Filter the row by date
    row = df[df["Date"] == date]
    if row.empty:
        return f"No entry found for {date}"

    if location.upper() == "SNEC":
        return {
            "MO": row["SNEC MO"].values[0],
            "Registrar": row["SNEC Registrar"].values[0]
        }
    elif location.upper() == "CGH":
        return {
            "MO": row["CGH MO"].values[0],
            "Registrar": row["CGH/SKH Registrar"].values[0],
            "Ward/Abn/DSOT": row["CGH Ward/Abn Results/DSOT"].values[0]
        }
    elif location.upper() == "KKH":
        return {
            "MO & Registrar": row["KKH MO & Registrar"].values[0],
            "Urgent CE & Blue Letters": row["KKH Urgent CE & Blue Letters"].values[0],
            "Review SDA/ADM": row["Review of SDA/ ADM Patients @ KKH"].values[0]
        }
    else:
        return "Invalid location. Choose from SNEC, CGH, or KKH."

def format_MO_on_call(result_dict, date, location):
    parts = [f"{role}: {name}" for role, name in result_dict.items()]
    text = f"On {date} at {location}, the on-call team is:\n" + "\n".join(parts)
    return text

def overall_MO_on_call_function(date, location):
  result = MO_on_call(date, location)
  output = format_MO_on_call(result, date, location)
  return(output)

# =====================
# Fellow on call Function
# =====================

def fellow_on_call(date, specialty):

    # Filter the row by date
    row = df[df["Date"] == date]
    if row.empty:
        return f"No entry found for {date}"

    # Map specialty aliases to exact column name
    specialty_map = {
        "CORNEA": "Cornea Fellow",
        "GLAUCOMA": "Glaucoma Fellow",
        "NEURO": "Neuro Fellow",
        "MEDICAL RETINA": "Medical Retina Fellow",
        "MR": "Medical Retina Fellow",
        "MED RET": "Medical Retina Fellow",
        "SURGICAL RETINA": "Surgical Retina Fellow",
        "VR": "Surgical Retina Fellow",
        "OCULOPLASTIC": "Occuloplastic Fellow (SNEC/SKH/KKH/CGH)",
        "OPLS": "Occuloplastic Fellow (SNEC/SKH/KKH/CGH)",
        "OCULOPLASTICS": "Occuloplastic Fellow (SNEC/SKH/KKH/CGH)",
        "UVEITIS": "Uveitis Fellow"
    }

    key = specialty.upper()
    if key in specialty_map:
        col_name = specialty_map[key]
        return row[col_name].values[0]
    else:
        return f"Invalid specialty. Choose from: {', '.join(set(specialty_map.keys()))}"

def format_fellow_on_call(fellow_name, date, specialty):
    # Special handling for Uveitis
    if specialty.upper() in ["UVEITIS"] and fellow_name.strip() == "-":
        return f"On {date}: \nThere is no Uveitis fellow rostered, please refer directly to the Uveitis Consultant on-call"

    return f"On {date}, the {specialty} on-call fellow is:\n{fellow_name}"

def overall_fellow_on_call_function(date, specialty):
    fellow_name = fellow_on_call(date, specialty)
    return format_fellow_on_call(fellow_name, date, specialty)

# =====================
# Consultant on call Function
# =====================

def consultant_on_call(date, specialty):

    row = df[df["Date"] == date]
    if row.empty:
        return f"No entry found for {date}"

    # Map input specialty to one or more columns
    specialty_map = {
        "GEN": ["SNEC General Consultant", "CGH Consultant"],
        "VR": ["SNEC Surgical Retina Consultant", "CGH VR Consultant"],
        "SR": ["SNEC Surgical Retina Consultant", "CGH VR Consultant"],
        "SURGICAL RETINA": ["SNEC Surgical Retina Consultant", "CGH VR Consultant"],
        "CORNEA": ["SNEC/CGH Cornea Consultant"],
        "OCULOPLASTIC": ["Oculoplastic Consultant \n (SNEC/SKH/KKH/CGH)"],
        "OCULOPLASTICS": ["Oculoplastic Consultant \n (SNEC/SKH/KKH/CGH)"],
        "OCULO": ["Oculoplastic Consultant \n (SNEC/SKH/KKH/CGH)"],
        "OPLS": ["Oculoplastic Consultant \n (SNEC/SKH/KKH/CGH)"],
        "NEURO": ["CGH Neuro"],
        "CASSIS": ["Cassis Consultant \n (SNEC/SKH/CGH/Bedok)"],
        "CAS": ["Cassis Consultant \n (SNEC/SKH/CGH/Bedok)"]
    }

    key = specialty.upper()
    if key not in specialty_map:
        return f"Invalid specialty. Choose from: {', '.join(specialty_map.keys())}"

    result = {}
    for col in specialty_map[key]:
        # Some columns might not exist in your dataframe — skip if missing
        if col in row.columns:
            result[col] = row[col].values[0]

    return result

def format_consultant_on_call(result_dict, date, specialty):
    """
    Nicely format consultant on-call output, multiple columns as separate lines.
    """
    if isinstance(result_dict, str):
        return result_dict

    parts = []
    for role, name in result_dict.items():
        # Clean role and name
        clean_role = role.split('\n')[0].strip()
        clean_name = str(name).replace('\n', ' ').strip()
        parts.append(f"{clean_role}: {clean_name}")


    text = f"On {date}, the {specialty} on-call consultant(s) are:\n" + "\n".join(parts)
    return text

def overall_consultant_on_call_function(date, specialty):
    result = consultant_on_call(date, specialty)
    return format_consultant_on_call(result, date, specialty)

# =====================
# NLP Parsing
# =====================
sgt = pytz.timezone("Asia/Singapore")

locations = ["SNEC", "CGH", "KKH"]

specialty_map = {
    "CORNEA": "Cornea",
    "GLAUCOMA": "Glaucoma",
    "NEURO": "Neuro",
    "MEDICAL RETINA": "Medical Retina",
    "MR": "Medical Retina",
    "MED RET": "Medical Retina",
    "SURGICAL RETINA": "Surgical Retina",
    "VR": "Surgical Retina",
    "SR": "Surgical Retina",
    "OCULOPLASTIC": "Oculoplastic",
    "OCULO": "Oculoplastic",
    "OPLS": "Oculoplastic",
    "OCULOPLASTICS": "Oculoplastic",
    "UVEITIS": "Uveitis",
    "GENERAL": "Gen",
    "GEN": "Gen",
    "CAS": "Cassis",
    "CASSIS": "Cassis",
    "CASIS": "Cassis"
}

def parse_query(query):
    query_lower = query.lower().replace("tmr", "tomorrow")

    specialty_key = next((key for key in specialty_map if key.lower() in query_lower), None)
    specialty = specialty_map.get(specialty_key) if specialty_key else None

    if specialty:
        if "fellow" in query_lower:
            call_type = "fellow"
        elif any(x in query_lower for x in ["cons", "consultant", "cas", 'cassis',"casis"]):
            call_type = "consultant"
        else:
            # Default to fellow if specialty is given but type not specified
            call_type = "fellow"
    else:
        # If no specialty but query mentions consultant keywords
        if "cons" in query_lower or "consultant" in query_lower:
            call_type = "consultant"
            specialty = "Gen"
        else:
            call_type = "MO"

    results = search_dates(query_lower, languages=['en'],
        settings={'PREFER_DATES_FROM': 'future', 'RELATIVE_BASE': datetime.now(sgt)})

    parsed_date = (results[0][1].date().strftime("%Y-%m-%d")
                   if results else datetime.now(sgt).date().strftime("%Y-%m-%d"))

    location = next((loc for loc in locations if loc.lower() in query_lower), "SNEC")

    return {"date": parsed_date, "location": location, "specialty": specialty, "type": call_type}


def overall_function(query):
    args = parse_query(query)
    if args["type"] == "fellow":
        return overall_fellow_on_call_function(args["date"], args["specialty"])
    elif args["type"] == "consultant":
        return overall_consultant_on_call_function(args["date"], args["specialty"])
    else:
        return overall_MO_on_call_function(args["date"], args["location"])

# =====================
# Phone Number Function
# =====================
def get_phone_number(text, contacts_df, score_cutoff=25):
    results = {}
    lines = re.split(r'[:\n]', text)
    for line in lines:
        line = line.strip()
        if not line or len(line) < 2:
            continue
        if any(label.lower() in line.lower() for label in ["mo", "registrar", "consultant", "fellow", "team"]):
            continue

        query_tokens = set(line.lower().split())
        candidates = contacts_df['NAME'][contacts_df['NAME'].str.lower().apply(lambda x: query_tokens <= set(x.split()))]

        if len(candidates) == 0:
            candidates = contacts_df['NAME']

        match, score, idx = process.extractOne(query=line, choices=candidates, scorer=fuzz.token_set_ratio)
        if score >= score_cutoff:
            results[contacts_df.loc[idx, "NAME"]] = contacts_df.loc[idx, "PHONE_FINAL"]
        else:
            results[line] = "No match found in database"

    results = str(results).strip("{}").replace("'", "").replace(", ", "\n")
    return "Suggested phone number(s):\n" + results

# -----------------------------
# Telegram Bot
# -----------------------------

def start(update, context):
    update.message.reply_text(
        "👋 Hi! I can tell you who's on call.\n"
        "Try sending queries like:\n"
        "- 'gen cons today'\n"
        "- 'Cornea fellow next Wednesday'\n"
        "- 'MO/Registrar at CGH tomorrow'"
    )


def handle_query(update, context):
    query = update.message.text
    try:
        response_names = overall_function(query)  # original on-call names
        phone_numbers = get_phone_number(response_names, contacts_df)
        final_reply = f"{response_names}\n\n{phone_numbers}"
    except Exception as e:
        response = f"⚠️ Something went wrong:\n{e}"
    update.message.reply_text(response)

# -----------------------------
# Telegram bot setup
# -----------------------------
TOKEN = os.getenv("BOT_TOKEN")
updater = Updater(TOKEN, use_context=True)
dp = updater.dispatcher

dp.add_handler(CommandHandler("start", start))
dp.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_query))

# Run polling in a background thread
threading.Thread(target=updater.start_polling, daemon=True).start()
print("Bot polling started ✅")

# -----------------------------
# Tiny web server to satisfy Render
# -----------------------------
app = Flask("bot")

@app.route("/")
def home():
    return "Bot is running ✅"

# Render provides PORT as an environment variable
port = int(os.environ.get("PORT", 10000))
app.run(host="0.0.0.0", port=port)
