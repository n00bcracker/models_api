from models.revenue_spark.app import app

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3333, debug=False)
