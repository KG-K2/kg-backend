import requests

BASE_URL = "http://127.0.0.1:8000"

print("Testing InfoBox API\n")
print("="*60)

# Step 1: Search for an artist
print("\n1. Searching for 'monet'...")
response = requests.get(f"{BASE_URL}/search?q=monet")
data = response.json()
print(f"Found {len(data['results'])} results")

# Step 2: Get artist details
if data['results']:
    for result in data['results']:
        if result['type'] == 'Artist':
            artist_id = result['id']
            print(f"\n2. Getting details for Artist ID: {artist_id}")
            response = requests.get(f"{BASE_URL}/artist/{artist_id}")
            
            if response.status_code == 200:
                artist = response.json()
                print(f"   Name: {artist['name']}")
                print(f"   Bio: {artist['bio'][:100]}..." if artist['bio'] else "   Bio: None")
                print(f"   Nationality: {artist['nationality']}")
                print(f"   Artworks: {artist['artworks_count']}")
                
                if artist['artworks']:
                    print(f"\n   Sample artworks:")
                    for artwork in artist['artworks'][:3]:
                        print(f"     - {artwork['title']}")
            break

# Step 3: Get artwork details
print("\n3. Searching for an artwork...")
response = requests.get(f"{BASE_URL}/search?q=starry")
data = response.json()

if data['results']:
    for result in data['results']:
        if result['type'] == 'Artwork':
            artwork_id = result['id']
            print(f"\n4. Getting details for Artwork ID: {artwork_id}")
            response = requests.get(f"{BASE_URL}/artwork/{artwork_id}")
            
            if response.status_code == 200:
                artwork = response.json()
                print(f"   Title: {artwork['title']}")
                print(f"   Artist: {artwork['artist_name']}")
                print(f"   URL: {artwork['url']}")
                
                if artwork['related_artworks']:
                    print(f"\n   Related artworks:")
                    for related in artwork['related_artworks']:
                        print(f"     - {related['title']}")
            break

print("\n" + "="*60)
print("✅ Test completed!")