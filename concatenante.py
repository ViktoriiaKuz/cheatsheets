import pandas as pd

def preprocess_chunk(chunk):
    # Здесь выполняется предварительная обработка данных для каждого чанка
    # Например, приведение к нижнему регистру, удаление пунктуации и т.д.
    processed_chunk = chunk.apply(lambda x: x.astype(str).str.lower())  # Пример предварительной обработки: приведение к нижнему регистру
    return processed_chunk

def preprocess_dataframe(df, chunk_size=1000):
    processed_chunks = []

    # Читаем исходный DataFrame чанками
    for chunk in pd.read_csv(df, chunksize=chunk_size):
        # Предварительная обработка каждого чанка
        processed_chunk = preprocess_chunk(chunk)
        processed_chunks.append(processed_chunk)

    # Склеиваем предварительно обработанные чанки в один DataFrame
    processed_df = pd.concat(processed_chunks)

    return processed_df

# Пример использования
input_df = 'your_input_dataframe.csv'  # Путь к вашему исходному DataFrame
preprocessed_df = preprocess_dataframe(input_df, chunk_size=10000)  #