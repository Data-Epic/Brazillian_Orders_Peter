import os
import sys

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from src.api import app
from src.api import app

# Run the Flask app
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080, debug=True)