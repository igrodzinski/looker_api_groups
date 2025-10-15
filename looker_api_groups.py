import time
import sys
import looker_sdk
from looker_sdk.sdk.api40 import models

# --- Konfiguracja ---
REFRESH_INTERVAL_MINUTES = 15
GROUP_NAME_PREFIX = "4"

# --- Inicjalizacja SDK ---
try:
    sdk = looker_sdk.init40()
except Exception as e:
    print(f"Błąd inicjalizacji Looker SDK. Sprawdź plik looker.ini: {e}")
    sys.exit(1)

def check_available_fields():
    """
    Funkcja diagnostyczna: Sprawdza i drukuje dostępne pola w widoku 'user'
    w modelu 'system__activity', aby zweryfikować dostęp do 'user.license_type'.
    """
    print("--- Diagnostyka: Sprawdzanie dostępnych pól ---")
    try:
        explore = sdk.lookml_model_explore(
            lookml_model_name="system__activity",
            explore_name="user"
        )
        print("Udało się połączyć z modelem 'system__activity'.")
        
        # Tworzymy listę nazw pól z wymiarów i miar
        dimensions = [field.name for field in explore.fields.dimensions]
        measures = [field.name for field in explore.fields.measures]
        all_fields = dimensions + measures
        
        # Sprawdzamy, czy kluczowe pole jest na liście
        if "user.license_type" in all_fields:
            print("✅ SUKCES: Pole 'user.license_type' jest dostępne dla Twojego użytkownika API.")
        else:
            print("⚠️ OSTRZEŻENIE: Pole 'user.license_type' NIE jest dostępne.")
            print("   Najczęstsza przyczyna: Użytkownik API nie ma uprawnienia 'see_system_activity'.")
            print("   Poproś administratora Lookera o nadanie tego uprawnienia.")

        print("-------------------------------------------------")
        
    except Exception as e:
        print(f"❌ BŁĄD: Nie można pobrać informacji o modelu 'system__activity'.")
        print(f"   Upewnij się, że Twój użytkownik API ma rolę z uprawnieniem 'see_system_activity'.")
        print(f"   Szczegóły błędu: {e}")
        print("-------------------------------------------------")
        # Jeśli nie możemy uzyskać dostępu do modelu, nie ma sensu kontynuować
        sys.exit(1)


def get_user_license_types():
    """
    Pobiera typy licencji dla wszystkich użytkowników.
    """
    try:
        query_body = models.WriteQuery(
            model="system__activity",
            view="user",
            fields=["user.id", "user.license_type"],
            limit="5000"
        )
        query_result_str = sdk.run_inline_query(
            result_format="json",
            body=query_body
        )
        # SDK zwraca string JSON, trzeba go zdeserializować
        import json
        query_result = json.loads(query_result_str)
        
        user_licenses = {}
        
        if not query_result or not isinstance(query_result, list):
            return {}
            
        # Sprawdzamy pierwszy wiersz, czy zawiera potrzebne pole
        if query_result and "user.license_type" not in query_result[0]:
            print("⚠️ OSTRZEŻENIE: Odpowiedź z API nie zawiera pola 'user.license_type'. Sprawdź uprawnienia.")
            return None # Zwracamy None, aby zasygnalizować problem

        for row in query_result:
            user_id = str(row.get("user.id"))
            license_type = row.get("user.license_type", "Standard") # Domyślnie 'Standard', jeśli brak
            user_licenses[user_id] = "Viewer" if license_type == "viewer" else "Standard"
            
        return user_licenses

    except Exception as e:
        print(f"Błąd podczas pobierania typów licencji użytkowników: {e}")
        return None

def analyze_groups():
    """
    Analizuje grupy, zlicza użytkowników i wyświetla wyniki.
    """
    print("\nRozpoczynam analizę grup...")
    
    user_licenses = get_user_license_types()
    if user_licenses is None: # Sprawdzamy, czy wystąpił błąd w poprzedniej funkcji
        print("Nie udało się pobrać informacji o licencjach z powodu braku danych lub błędu. Prerywam ten cykl.")
        return

    try:
        all_groups = sdk.all_groups(fields="id,name")
        filtered_groups = [group for group in all_groups if group.name.startswith(GROUP_NAME_PREFIX)]

        if not filtered_groups:
            print(f"Nie znaleziono grup o nazwie zaczynającej się od '{GROUP_NAME_PREFIX}'.")
            return

        print("\n--- Raport Licencji Użytkowników w Grupach ---")
        for group in filtered_groups:
            viewer_count = 0
            standard_count = 0
            
            group_users = sdk.all_group_users(group_id=group.id)

            for user in group_users:
                license_type = user_licenses.get(str(user.id), "Standard") # Bezpieczniejsze pobieranie
                if license_type == "Viewer":
                    viewer_count += 1
                else: # Wszyscy inni (w tym ci bez licencji w mapie) będą Standard
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
    # Uruchom diagnostykę tylko raz na początku
    check_available_fields()
    
    try:
        while True:
            analyze_groups()
            print(f"\nSkrypt zostanie ponownie uruchomiony za {REFRESH_INTERVAL_MINUTES} minut.")
            print("Naciśnij Ctrl+C, aby zakończyć.")
            time.sleep(REFRESH_INTERVAL_MINUTES * 60)
    except KeyboardInterrupt:
        print("\n\nPrzerwanie przez użytkownika. Zamykanie skryptu... 👋")
        sys.exit(0)
