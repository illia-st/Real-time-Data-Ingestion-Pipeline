https://www.kaggle.com/datasets/waqi786/e-commerce-clickstream-and-transaction-dataset/data

https://www.kaggle.com/datasets/olxdatascience/olx-jobs-interactions/data

Let me think a little bit about what my producer, consumer should do.
1) Read from dataset - by chunks
2) Load the data to the topic in kafka
3) Consumer needs to read that data and save it somewhere (Amazon S3)