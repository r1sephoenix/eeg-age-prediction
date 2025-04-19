import numpy as np

def run(input_file, output_file):
    # Заглушка для предобработки
    data = np.random.rand(64, 1000)
    np.save(output_file, data)

def run_inline(input_file):
    # Препроцессинг без сохранения
    return np.random.rand(64, 1000)