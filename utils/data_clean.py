import csv
import re
import os

# --- KONFIGURASI FILE ---
FILES = {
    "artists": "artists.csv",
    "info": "info_dataset.csv",
    "artworks": "artwork_dataset.csv"
}

OUTPUT_FILES = {
    "artists": "cleaned_artists.csv",
    "info": "cleaned_info.csv",
    "artworks": "cleaned_artworks.csv"
}

MEDIUM_KEYWORDS = [
    "oil", "canvas", "panel", "wood", "tempera", "fresco", "paper", 
    "bronze", "marble", "copper", "gold", "silver", "sketch", "drawing",
    "watercolor", "ink", "chalk", "graphite", "sculpture", "statue",
    "poplar", "oak", "masonite", "acrylic", "pastel", "etching", "engraving",
    "lithograph", "gouache", "stone", "photo", "watercolour"
]

# --- HELPER FUNCTIONS ---

def normalize_name(name):
    """
    Ubah "AACHEN, Hans von" -> "Hans von Aachen".
    Ubah "Amedeo Modigliani" -> "Amedeo Modigliani" (Tetap).
    """
    if not name:
        return "Unknown Artist"
    
    # Hapus spasi di awal/akhir
    clean_name = name.strip()
    
    # Cek apakah formatnya "LAST, First"
    if "," in clean_name:
        parts = clean_name.split(",", 1) # Pisah hanya pada koma pertama
        if len(parts) == 2:
            last = parts[0].strip()
            first = parts[1].strip()
            # Gabung jadi "First Last"
            # Title case biar rapi (misal: AACHEN -> Aachen)
            return f"{first} {last.title()}"
            
    return clean_name

def parse_years_advanced(text):
    if not text:
        return None, None
    clean_text = text.replace('(', '').replace(')', '').strip()
    pattern = re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])s?\b')
    years = pattern.findall(clean_text)
    
    if not years:
        return None, None
    
    birth = int(min(years))
    death = int(max(years)) if len(years) > 1 else None
    
    if death and death < birth:
        death = None 
    return birth, death

def smart_parse_metadata(raw_text):
    if not raw_text:
        return {
            "year_created": "Unknown Year", 
            "medium": "Unknown Medium", 
            "dimensions": "Unknown Dimensions", 
            "location": "Unknown Location"
        }

    parts = [p.strip() for p in raw_text.split(',')]
    found_year = None
    found_medium = None
    found_dims = None
    remaining_parts = []

    dim_pattern = re.compile(r'(\d+(\.\d+)?\s*x\s*\d+(\.\d+)?)|(\b\d+(\.\d+)?\s*(cm|mm|in|inch|ft|m)\b)|(height|width|depth|diameter)', re.IGNORECASE)
    year_pattern = re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])s?\b')
    address_pattern = re.compile(r'(museum|gallery|collection|palace|cathedral|church|university|library|street|road|avenue|st\.|rd\.|ave\.)', re.IGNORECASE)

    for part in parts:
        is_identified = False
        lower_part = part.lower()

        if not found_dims and dim_pattern.search(lower_part):
            found_dims = part
            is_identified = True
        elif not found_medium and any(k in lower_part for k in MEDIUM_KEYWORDS):
            found_medium = part
            is_identified = True
        elif not found_year and year_pattern.search(part) and len(part) < 50:
            if not address_pattern.search(lower_part):
                match = year_pattern.search(part)
                found_year = match.group(1) 
                is_identified = True
        
        if not is_identified:
            remaining_parts.append(part)

    valid_locations = [p for p in remaining_parts if len(p) > 2 and not p.isdigit()]
    final_location = ", ".join(valid_locations) if valid_locations else "Unknown Location"

    return {
        "year_created": found_year if found_year else "Unknown Year",
        "medium": found_medium if found_medium else "Unknown Medium",
        "dimensions": found_dims if found_dims else "Unknown Dimensions",
        "location": final_location
    }

# --- MAIN PROCESS ---

def clean_all_data():
    print("🚀 Memulai Smart Cleaning & Normalisasi Nama...")

    # 1. CLEAN ARTISTS (VIP)
    print(f"   Cleaning {FILES['artists']}...")
    try:
        with open(FILES['artists'], 'r', encoding='utf-8') as fin, \
             open(OUTPUT_FILES['artists'], 'w', newline='', encoding='utf-8') as fout:
            reader = csv.DictReader(fin)
            # Kita tambah kolom 'clean_name' biar ETL tinggal pake
            fieldnames = reader.fieldnames + ['birth_year_clean', 'death_year_clean', 'clean_name']
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                row['clean_name'] = normalize_name(row.get('name', ''))
                b, d = parse_years_advanced(row.get('years', ''))
                row['birth_year_clean'] = b
                row['death_year_clean'] = d
                writer.writerow(row)
    except FileNotFoundError:
        print(f"❌ Skip artists: File {FILES['artists']} tidak ditemukan.")

    # 2. CLEAN INFO (BASE)
    print(f"   Cleaning {FILES['info']}...")
    try:
        with open(FILES['info'], 'r', encoding='utf-8') as fin, \
             open(OUTPUT_FILES['info'], 'w', newline='', encoding='utf-8') as fout:
            reader = csv.DictReader(fin)
            fieldnames = reader.fieldnames + ['birth_year_clean', 'death_year_clean', 'clean_name']
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                # Kolomnya 'artist' bukan 'name'
                row['clean_name'] = normalize_name(row.get('artist', ''))
                b, d = parse_years_advanced(row.get('born-died', ''))
                row['birth_year_clean'] = b
                row['death_year_clean'] = d
                writer.writerow(row)
    except FileNotFoundError:
        print(f"❌ Skip info: File {FILES['info']} tidak ditemukan.")

    # 3. CLEAN ARTWORKS
    print(f"   Cleaning {FILES['artworks']}...")
    try:
        with open(FILES['artworks'], 'r', encoding='utf-8') as fin, \
             open(OUTPUT_FILES['artworks'], 'w', newline='', encoding='utf-8') as fout:
            reader = csv.DictReader(fin)
            new_cols = ['clean_year', 'clean_medium', 'clean_dimensions', 'clean_location', 'clean_url', 'clean_artist_name']
            fieldnames = reader.fieldnames + new_cols
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in reader:
                meta = smart_parse_metadata(row.get('picture data', ''))
                row['clean_year'] = meta['year_created']
                row['clean_medium'] = meta['medium']
                row['clean_dimensions'] = meta['dimensions']
                row['clean_location'] = meta['location']
                row['clean_url'] = row.get('jpg url', '').strip().replace('"', '')
                
                # Normalisasi nama artist di artwork juga! PENTING!
                row['clean_artist_name'] = normalize_name(row.get('artist', ''))
                
                writer.writerow(row)
    except FileNotFoundError:
        print(f"❌ Skip artworks: File {FILES['artworks']} tidak ditemukan.")

    print("\n🎉 Selesai! Data nama sudah seragam (First Last).")

if __name__ == "__main__":
    clean_all_data()