import os
import sys
from src.api import app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Run the Flask app
if __name__ == '__main__':
    app.run(host="0.0.0.0",port=8000, debug=True)