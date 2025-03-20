import os
import nbformat as nbf
from nbconvert.preprocessors import ExecutePreprocessor
import sys
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Получаем значение переменной окружения
dune_api = os.getenv('DUNE_API')
if not dune_api:
    print("Ошибка: Переменная окружения DUNE_API не установлена")
    print("Пожалуйста, добавьте DUNE_API в файл .env")
    sys.exit(1)

# Читаем существующий notebook
with open('script.ipynb') as f:
    nb = nbf.read(f, as_version=4)

# Добавляем ячейку с установкой переменной окружения в начало notebook
env_cell = nbf.v4.new_code_cell(f'''import os
os.environ["DUNE_API"] = "{dune_api}"
''')
nb.cells.insert(0, env_cell)

# Сохраняем временный notebook
with open('temp_script.ipynb', 'w') as f:
    nbf.write(nb, f)

# Запускаем notebook
ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
ep.preprocess('temp_script.ipynb', {})

# Удаляем временный файл
os.remove('temp_script.ipynb') 