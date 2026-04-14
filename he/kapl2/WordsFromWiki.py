import requests
import time

def get_hebrew_lemmas(category_name):
    url = "https://en.wiktionary.org/w/api.php"
    lemmas = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": f"Category:{category_name}",
        "cmlimit": "max",
        "format": "json"
    }

    headers = {
        "User-Agent": "HebrewLemma/1.0 (contact: your@email.com) Python-requests"
    }


    print(f"Retrieving {category_name}...")
    
    while True:
        response = requests.get(url, params=params, headers = headers)
        response.raise_for_status()
        data = response.json()
        
        # Extract the titles (these are the normalized base forms)
        for member in data.get("query", {}).get("categorymembers", []):
            if not member["title"].startswith("קטגוריה:"): # Skip subcategories
                lemmas.append(member["title"])
                
        # Handle pagination
        if "continue" in data:
            params["cmcontinue"] = data["continue"]["cmcontinue"]
            time.sleep(0.5) # Be respectful to the API
        else:
            break
    
    forbidden_symbols = {',', '.', '״', ' ', '־', '׳'}

    lemmas = [
        w for w in lemmas 
        if 4 <= len(w) <= 6                     # Length constraint
        and not any(char in forbidden_symbols for char in w) # Symbol constraint
    ]            

    return lemmas



verbs = get_hebrew_lemmas("Hebrew_verbs")
nouns = get_hebrew_lemmas("Hebrew_nouns")
adjectives = get_hebrew_lemmas("Hebrew_adjectives")

print(f"Retrieved {len(verbs)} verbs.")
print(f"Retrieved {len(nouns)} nouns.")
print(f"Retrieved {len(adjectives)} adjectives.")

# Save to text files for NLP use
with open("hebrew_verbs.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(verbs))
    
with open("hebrew_nouns.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(nouns))

with open("hebrew_adjectives.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(adjectives))