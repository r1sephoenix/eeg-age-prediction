import typer
from src import preprocess, inference

app = typer.Typer()

@app.command()
def preprocess_eeg(input_file: str, output_file: str):
    preprocess.run(input_file, output_file)

@app.command()
def train(model_type: str = "sklearn"):
    if model_type == "sklearn":
        print("Training sklearn model...")
    elif model_type == "pytorch":
        print("Training pytorch model...")

@app.command()
def infer(eeg_file: str, model_path: str):
    age = inference.run(eeg_file, model_path)
    print(f"Predicted age: {age}")

@app.command()
def predict(raw_file: str, model_path: str):
    print("[INFO] Starting preprocessing...")
    processed = preprocess.run_inline(raw_file)
    print("[INFO] Running inference...")
    age = inference.run_from_array(processed, model_path)
    print(f"Predicted age: {age}")

if __name__ == "__main__":
    app()