# --- Montar google Drive --- #
from google.colab import drive
# 1. Desmontar primero para limpiar el caché
drive.flush_and_unmount()
# 2. Volver a montar forzosamente
drive.mount('/content/drive', force_remount=True)

# --- Librerias --- #
import pandas as pd
import numpy as np
