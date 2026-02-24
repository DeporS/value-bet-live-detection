import pandas as pd

# Podajemy ścieżkę do całego folderu, a nie pojedynczego pliku!
# Pandas automatycznie sklei wszystkie pliki z podkatalogów (np. match_id=...)
folder_path = "data/training_set"

try:
    # Wczytanie całego zbioru danych
    df = pd.read_parquet(folder_path)
    
    # Sortowanie chronologiczne po ID meczu i czasie rozpoczęcia okna
    df = df.sort_values(by=['match_id', 'window_start'])
    
    # Wyświetlanie wszystkich kolumn
    pd.set_option('display.max_columns', None)
    
    print(f"Łączna liczba zapisanych wierszy (okien) w bazie: {len(df)}")
    
    print("\n--- Liczba okien zebrana dla poszczególnych meczów ---")
    print(df.groupby('match_id').size())
    
    print("\n--- Ostatnie 15 okien chronologicznie (podgląd dynamiki) ---")
    # Wybieramy tylko kilka kluczowych kolumn, żeby czytelnie zmieściły się w konsoli
    preview_cols = [
        'match_id', 'current_minute', 'current_second', 
        'home_goals', 'away_goals', 
        'momentum_home_total_shots', 'momentum_away_total_shots',
        'momentum_home_possession'
    ]
    
    print(df[preview_cols].tail(15))

except Exception as e:
    print(f"Błąd podczas wczytywania plików: {e}")