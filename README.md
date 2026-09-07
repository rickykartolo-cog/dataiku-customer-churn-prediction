# Customer Churn Prediction

## Databricks migration

The Dataiku flow is also available as a PySpark package and a three-task
Databricks Asset Bundle Job. See [docs/MIGRATION.md](docs/MIGRATION.md) for
the recipe mapping, translation details, local test commands, and deployment
notes. Override `--base_output_path` with a Unity Catalog Volume path such as
`/Volumes/<catalog>/<schema>/churn` or a DBFS path when running the tasks.

## Overview
This project focuses on predicting telecom customer churn using Machine Learning models in **Dataiku DSS**. It combines **K-Means clustering** for customer segmentation and **Random Forest classification** for churn prediction, helping telecom companies identify at-risk customers and reduce revenue loss.

## Objectives
- Analyze customer behavior to detect churn patterns.
- Clean and prepare data for modeling.
- Segment customers using K-Means clustering.
- Predict churn probability using Random Forest.
- Visualize key insights through Dataiku dashboards.

## Tools & Technologies
- **Platform:** Dataiku DSS
- **Language:** Python
- **Libraries:** pandas, numpy, scikit-learn
- **Dataset:** Telecom Churn Dataset (Kaggle)

## Methodology
1. **Data Import & Cleaning**  
   - Imported the dataset into Dataiku DSS and handled missing values, duplicates, and categorical encoding.

2. **Exploratory Data Analysis (EDA)**  
   - Analyzed customer demographics, usage patterns, and service interactions to identify potential churn indicators.

3. **Customer Segmentation**  
   - Applied K-Means clustering to segment customers based on usage patterns and service interactions.

4. **Churn Prediction**  
   - Trained a Random Forest classifier to predict churn probability.
   - Evaluated model performance using accuracy, precision, recall, and feature importance.

5. **Insights & Visualization**  
   - Created dashboards in Dataiku DSS to visualize clusters, churn probabilities, and key churn drivers.

## Key Findings
- Major churn indicators included:  
  - Frequent customer service calls  
  - Short contract duration  
  - High service usage (minutes, data)  
- Combining segmentation with classification improved model interpretability and allowed targeted retention strategies.

## How to Run
1. Open the project in Dataiku DSS.
2. Import the telecom churn dataset.
3. Run the data preparation flow.
4. Execute the K-Means clustering and Random Forest recipes.
5. Explore dashboards for insights and analysis.

## Conclusion
Accurate churn prediction enables telecom companies to implement data-driven retention strategies, reduce revenue loss, and improve customer satisfaction.

## Author
**Shreya Thakur**
