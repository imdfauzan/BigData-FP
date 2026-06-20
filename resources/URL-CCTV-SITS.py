import requests
import binascii
from Crypto.Cipher import AES
import urllib3

# Menghilangkan warning TLS/SSL Insecure di terminal
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_URL = "https://dishub.surabaya.go.id/p56/api/sits/cctv2.php"
KEY = b"0a1b2c3d4e5f6789"
IV = b"f0e1d2c3b4a59876"

def decrypt_rtsp(hex_string):
    try:
        encrypted_bytes = binascii.unhexlify(hex_string)
        cipher = AES.new(KEY, AES.MODE_CBC, IV)
        decrypted_bytes = cipher.decrypt(encrypted_bytes)
        return decrypted_bytes.rstrip(b'\0').decode('utf-8')
    except Exception as e:
        return f"Gagal dekripsi: {e}"

print("🔄 Menghubungkan ke API SITS dan mendekripsi database...")
response = requests.get(API_URL, verify=False)

if response.status_code == 200:
    data = response.json()
    cctv_list = data.get('cctv', [])
    
    # Filter hanya CCTV yang punya data RTSP
    valid_cctv = [c for c in cctv_list if c.get('rtsp')]
    print(f"✅ Berhasil menemukan {len(valid_cctv)} CCTV aktif!\n")
    
    while True:
        print("=" * 40)
        print("       CCTV SITS URL DECRYPTOR")
        print("=" * 40)
        print("1. Tampilkan Semua CCTV")
        print("2. Cari CCTV")
        print("3. Exit")
        print("-" * 40)
        
        pilihan = input("Pilih menu (1/2/3): ").strip()
        
        if pilihan == '1':
            print("\n--- DAFTAR SELURUH CCTV ---")
            for idx, cctv in enumerate(valid_cctv, 1):
                print(f"[{idx}] {cctv['nama_cctv']}")
            
            try:
                pilih_nomor = int(input("\nMasukkan Nomor CCTV yang ingin diambil URL-nya: "))
                if 1 <= pilih_nomor <= len(valid_cctv):
                    target = valid_cctv[pilih_nomor - 1]
                    url_asli = decrypt_rtsp(target['rtsp'])
                    print("\nURL berhasil di decrypt!")
                    print(f"CCTV : {target['nama_cctv']}")
                    print(f"URL: {url_asli}")
                else:
                    print("❌ Nomor di luar jangkauan!\n")
            except ValueError:
                print("❌ Masukkan angka indeks yang valid!\n")
                
        elif pilihan == '2':
            keyword = input("\nMasukkan kata kunci pencarian: ").strip().upper()
            # Filter pencarian berdasarkan keyword
            hasil_cari = [(idx, c) for idx, c in enumerate(valid_cctv, 1) if keyword in c['nama_cctv'].upper()]
            
            if not hasil_cari:
                print(f"❌ Tidak ditemukan nama CCTV dengan kata kunci '{keyword}'\n")
                continue
                
            print(f"\n--- HASIL PENCARIAN BERDASARKAN '{keyword}' ---")
            for urutan, (original_idx, cctv) in enumerate(hasil_cari, 1):
                print(f"[{urutan}] {cctv['nama_cctv']}")
                
            try:
                pilih_nomor = int(input("\nPilih nomor dari hasil pencarian di atas: "))
                if 1 <= pilih_nomor <= len(hasil_cari):
                    _, target = hasil_cari[pilih_nomor - 1]
                    url_asli = decrypt_rtsp(target['rtsp'])
                    print(f"📍 NAMA CCTV : {target['nama_cctv']}")
                    print(f"URL: {url_asli}")
                else:
                    print("❌ Nomor di luar jangkauan!\n")
            except ValueError:
                print("❌ Masukkan angka indeks yang valid!\n")
                
        elif pilihan == '3':
            print("\nTerima kasih! Selamat melanjutkan pipeline Kafka & YOLO-mu. Sukses FP Big Data-nya!")
            break
        else:
            print("❌ Pilihan menu salah, coba lagi.\n")
else:
    print(f"❌ Gagal mengambil data dari API SITS. HTTP Status: {response.status_code}")