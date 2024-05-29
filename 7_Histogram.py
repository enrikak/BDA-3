import pandas as pd
import matplotlib.pyplot as plt
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def generate_histogram_from_csv(file_path):
    # Loading the delta_t values from the CSV file
    df = pd.read_csv(file_path)

    # Ensuring the 'delta_t' column exists
    if 'delta_t' not in df.columns:
        logging.error(f"'delta_t' column not found in {file_path}")
        return

    delta_t_values = df['delta_t']

    # Printing statistics for debugging
    logging.info(f"Statistics of delta_t values: \n{df.describe()}")

    # Generating histogram
    plt.figure(figsize=(10, 6))
    plt.hist(delta_t_values, bins=50, edgecolor='black')
    plt.title('Histogram of Delta t (time difference) values')
    plt.xlabel('Delta t (milliseconds)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.yscale('log')  # Using a logarithmic scale to better visualize the distribution

    # Ensuring x-axis is labeled in milliseconds
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main():
    # Path to the CSV file containing delta_t values
    file_path = 'delta_time.csv'

    # Generating histogram from the CSV file
    generate_histogram_from_csv(file_path)


if __name__ == '__main__':
    main()

# Insights of a histogram
# There are two distinct peaks in the histogram, where one peak is very close to zero, and another peak is further to the right.
# The large peak near zero indicates that the majority of the recorded delta_t values are very small, suggesting that many vessels have frequent data points recorded in quick succession, often with intervals of a few milliseconds.
# The second peak around 20,000,000 milliseconds indicates another significant grouping of data points that are spaced apart by a substantial amount of time (around 5.56 hours).
# The majority of vessels seem to be reporting data very frequently, possibly because of automated tracking systems that record vessel positions and other information continuously or at very short intervals.
# The distinct gap between the two peaks suggests a change in the operational patterns or conditions under which data is recorded.