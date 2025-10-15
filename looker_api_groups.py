import time
import looker_sdk
from looker_sdk import models
import sys # Dodajemy import sys, aby móc elegancko zakończyć program

# --- Konfiguracja ---
REFRESH_INTERVAL_MINUTES = 15 # Co ile minut skrypt ma się uruchamiać
GROUP_NAME_PREFIX = "4" # Prefix nazw grup do wyszukania

# Inicjalizacja Looker SDK
# Upewnij się, że masz skonfigurowany plik looker.ini w tym samym katalogu
try:
    sdk = looker_sdk.init40()
except Exception as e:
    print(f"Błąd inicjalizacji Looker SDK. Sprawdź plik looker.ini: {e}")
    sys.exit(1) # Zakończ, jeśli nie można połączyć się z API

def get_user_license_types():
    """
    Pobiera typy licencji dla wszystkich użytkowników za pomocą System Activity Explore.
    Zwraca słownik mapujący user_id na typ licencji ('Viewer' lub 'Standard').
    """
    try:
        query_body = models.WriteQuery(
            model="system__activity",
            view="user",
            fields=["user.id", "user.license_type"],
            limit="5000" # Zwiększ w razie potrzeby
        )
        query_result = sdk.run_inline_query(
            result_format="json",
            body=query_body
        )
        
        user_licenses = {}
        # Sprawdzamy, czy query_result nie jest pusty i jest listą
        if query_result and isinstance(query_result, list):
            for row in query_result:
                user_id = str(row.get("user.id"))
                license_type = row.get("user.license_type")
                user_licenses[user_id] = "Viewer" if license_type == "viewer" else "Standard"
        return user_licenses

    except Exception as e:
        print(f"Błąd podczas pobierania typów licencji użytkowników: {e}")
        return {}

def analyze_groups():
    """
    Analizuje grupy, zlicza użytkowników i wyświetla wyniki.
    """
    print("Rozpoczynam analizę grup...")
    
    user_licenses = get_user_license_types()
    if not user_licenses:
        print("Nie udało się pobrać informacji o licencjach. Prerywam analizę.")
        return

    try:
        # Pobierz wszystkie grupy
        all_groups = sdk.all_groups()

        # Filtruj grupy po prefiksie nazwy
        filtered_groups = [group for group in all_groups if group.name.startswith(GROUP_NAME_PREFIX)]

        if not filtered_groups:
            print(f"Nie znaleziono grup o nazwie zaczynającej się od '{GROUP_NAME_PREFIX}'.")
            return

        print("\n--- Raport Licencji Użytkowników w Grupach ---")
        for group in filtered_groups:
            viewer_count = 0
            standard_count = 0
            
            # Pobierz użytkowników dla danej grupy
            group_users = sdk.all_group_users(group_id=group.id)

            for user in group_users:
                license_type = user_licenses.get(str(user.id))
                if license_type == "Viewer":
                    viewer_count += 1
                elif license_type == "Standard":
                    standard_count += 1
            
            total_users = viewer_count + standard_count

            print(f"\nGrupa: {group.name} (ID: {group.id})")
            print(f"  Liczba użytkowników 'Standard': {standard_count}")
            print(f"  Liczba użytkowników 'Viewer': {viewer_count}")
            print(f"  Suma użytkowników: {total_users}")
        print("\n--- Koniec Raportu ---")

    except Exception as e:
        print(f"Wystąpił błąd podczas analizy grup: {e}")

if __name__ == "__main__":
    try:
        while True:
            analyze_groups()
            print(f"\nSkrypt zostanie ponownie uruchomiony za {REFRESH_INTERVAL_MINUTES} minut.")
            print("Naciśnij Ctrl+C, aby zakończyć.")
            time.sleep(REFRESH_INTERVAL_MINUTES * 60)
    except KeyboardInterrupt:
        print("\n\nPrzerwanie przez użytkownika. Zamykanie skryptu... 👋")
        sys.exit(0)
