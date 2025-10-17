import os
import pandas as pd
from datetime import datetime
import pytz
from dateparser.search import search_dates
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import nest_asyncio

# =====================
# Google Sheet setup
# =====================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1mZx6j7hEuJYeuYM0i79XUQ1gRJTg1I8cTnIgIGBHDyI/edit?gid=43119476#gid=43119476"
SHEET_ID = SHEET_URL.split("/d/")[1].split("/")[0]
TABS = ["MO_Reg", "Fellows", "Consultants"]

def load_sheet():
    dfs = []
    for tab in TABS:
        csv_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={tab}"
        df = pd.read_csv(csv_url)
        df.columns = df.columns.str.strip()
        dfs.append(df)

    base = dfs[0][["Date", "Day"]].copy()
    for df in dfs:
        extra_cols = [c for c in df.columns if c not in ["Date", "Day"]]
        base = pd.concat([base, df[extra_cols]], axis=1)
    base["Date"] = pd.to_datetime(base["Date"])
    return base

df = load_sheet()

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
    "CASSIS": "Cassis"
}

def parse_query(query):
    query_lower = query.lower().replace("tmr", "tomorrow")

    specialty_key = next((key for key in specialty_map if key.lower() in query_lower), None)
    specialty = specialty_map.get(specialty_key) if specialty_key else None

    if specialty:
        if "fellow" in query_lower:
            call_type = "fellow"
        elif "cons" in query_lower or "consultant" in query_lower:
            call_type = "consultant"
        else:
            call_type = "fellow"
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

# -----------------------------
# Telegram Bot
# -----------------------------

nest_asyncio.apply()

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
        response = overall_function(query)
    except Exception as e:
        response = f"⚠️ Something went wrong:\n{e}"
    update.message.reply_text(response)


# -----------------------------
# Main loop
# -----------------------------

if __name__ == "__main__":
    TOKEN = os.getenv("BOT_TOKEN")
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_query))

    updater.start_polling()
    print("Bot started ✅")
    updater.idle()
