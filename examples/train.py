# pip: numpy, pandas, scikit-learn
"""
Example ML training script for Avix.
Submit with: avix job submit examples/train.py --from-py --backend local-docker
"""

import argparse
import numpy as np

def main():
    parser = argparse.ArgumentParser(description='Simple ML training example')
    parser.add_argument('--epochs', type=int, default=10, help='Number of epochs')
    parser.add_argument('--learning-rate', type=float, default=0.01, help='Learning rate')
    args = parser.parse_args()
    
    print(f"Starting training with {args.epochs} epochs, lr={args.learning_rate}")
    
    # Simulate training
    for epoch in range(args.epochs):
        loss = 1.0 / (epoch + 1) + np.random.random() * 0.1
        accuracy = 1.0 - loss + np.random.random() * 0.05
        print(f"Epoch {epoch+1}/{args.epochs} - loss: {loss:.4f} - accuracy: {accuracy:.4f}")
    
    print("Training complete!")

if __name__ == "__main__":
    main()
