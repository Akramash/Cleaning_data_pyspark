# Cleaning Data with PySpark

This project involves cleaning and preprocessing a dataset containing order information using PySpark. The cleaned data is prepared for further use, such as building a demand forecasting model. The dataset is stored in a Parquet file format.

## Project Structure

- `clean_orders.py`: Main script for cleaning and preprocessing the data.
- `orders_data.parquet`: Input data file containing raw order information.
- `output_directory/orders_data_clean.parquet`: Output directory containing the cleaned data.

## Getting Started

### Prerequisites

- Python 3.6 or higher
- PySpark
- Pandas
- Java (JDK 11 or higher)

### Installation

1. **Clone the repository**:
    ```sh
    git clone git@github.com:Akramash/Cleaning_data_pyspark.git
    cd Cleaning_data_pyspark
    ```

2. **Set up your environment**:
    Ensure you have Java installed:
    ```sh
    brew install openjdk@11
    ```

    Add the following to your `~/.zshrc` or `~/.bash_profile`:
    ```sh
    export JAVA_HOME=$(/usr/libexec/java_home -v 11)
    export PATH=$JAVA_HOME/bin:$PATH
    ```

    Source your profile to apply changes:
    ```sh
    source ~/.zshrc
    ```

3. **Install Python dependencies**:
    ```sh
    pip3 install pyspark pandas
    ```

## Usage

1. **Place the Parquet file**:
    Ensure the `orders_data.parquet` file is placed in the project directory.

2. **Run the cleaning script**:
    ```sh
    python3 clean_orders.py
    ```

    This will process the `orders_data.parquet` file and save the cleaned data to `output_directory/orders_data_clean.parquet`.

## Script Details

### `clean_orders.py`

The script performs the following steps:

1. **Initialize Spark Session**:
    Initializes a Spark session for data processing.

2. **Read the Parquet File**:
    Reads the input Parquet file `orders_data.parquet` into a Spark DataFrame.

3. **Filter Orders by Time**:
    Removes orders placed between midnight (12:00 AM) and 5:00 AM inclusive.

4. **Add `time_of_day` Column**:
    Adds a column categorizing orders into 'morning', 'afternoon', and 'evening' based on the order time.

5. **Convert `order_date` to Date**:
    Converts the `order_date` column from timestamp to date, removing the time part.

6. **Remove Specific Products and Standardize Case**:
    Removes rows where the product name contains 'TV' and ensures all product names and categories are in lowercase.

7. **Extract and Add `purchase_state` Column**:
    Extracts the state abbreviation from the purchase address and adds it as a new column.

8. **Save Cleaned Data**:
    Saves the cleaned DataFrame to a new Parquet file `output_directory/orders_data_clean.parquet`.

9. **Convert to Pandas DataFrame**:
    Converts the cleaned Spark DataFrame to a Pandas DataFrame for better visualization and displays the first 20 rows.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

