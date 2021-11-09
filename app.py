#################################################
# Import Modules
#################################################

from flask import Flask
from flask import render_template
from flask import redirect
from flask import jsonify
from flask import request
from collections import OrderedDict
import math
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
import json
import pickle
from datetime import datetime

app = Flask(__name__)


#################################################
# Flask Routes
#################################################

# Customer Segmentation Route
@app.route("/api/customer/segmentation/<age>/<spending_score>", methods=["GET"])
def customerSegmentation(age,spending_score):
    ''' Use KNN to classify customers based on 'age' and 'spending_score' as parameters: '''
    
    # Load model
    with open('models/knn','rb') as file:
        km = pickle.load(file)
        print('model loaded')

    # Step 1. Input data along with a sample to run the algorithm
    data = {'age':  [18,20,int(age)],
        'spending_score': [15,80,int(spending_score)]
        }
    prediction = pd.DataFrame(data)
    
    # Step2 . Scale features
    prediction[["age_scaled","spending_score_scaled"]] = StandardScaler().fit_transform(prediction[["age","spending_score"]])
    prediction_scaled = prediction.loc[:,["age_scaled","spending_score_scaled"]]
    
    # Step 3. Fit the model and predict cluster
    y_means = km.fit_predict(prediction_scaled)
    prediction["cluster"] = y_means
    prediction["target"] = y_means
    
    # == Clustering groups === 
    print("Cluster Classification")
    print(f" Cluster 2 = High purchasing activity and Young = Not for Marketing Campaing")
    print(f" Cluster 1 = Low purchasing activity and Young = Marketing Campaing")
    print(f" Cluster 0 = Fluctuating purchasing activity and Elder = Not for Marketing Campaing")
    prediction.loc[prediction["cluster"] == 1, ["target"]] = 'Target Customer'
    prediction.loc[prediction["cluster"] != 1, ["target"]] = 'Not Target Customer'
    print(prediction)
    
    # Send json result
    return jsonify(json.loads(prediction.loc[2:,["age","spending_score","cluster","target"]].to_json(orient='records')))




@app.route("/api/item/recommendation/<customer_id>", methods=["GET"])
def itemRecommendation(customer_id):    
    ''' Use SVD to predict rating for unseen products by a particular user '''
    # Create an engine instance
    with open("dags/ops/config/config.json", "r") as config:
        print('Reading config file')
        configuration = json.loads(config.read())
    alchemyEngine = create_engine(f'postgresql+psycopg2://{configuration["db_connection"]["user"]}:{configuration["db_connection"]["password"]}@{configuration["db_connection"]["host"]}:{configuration["db_connection"]["port"]}/{configuration["db_connection"]["database"]}',
                                    pool_recycle=3600)

    dbConnection = alchemyEngine.connect()
    
    # Get products that have not been rated by the user
    df = pd.read_sql('select * from ratings', dbConnection)
    products = pd.read_sql(f'''select p.product_id, p.product_name, pc.product_category_name 
    from (
        select r.product_id, r.rating 
        from ratings r
        where customer_id = {int(customer_id)}
        ) A
        right outer join product p 
            on A.product_id = p.product_id
        left join product_category pc 
            on pc.product_category_id = p.product_category_id 
    where A.product_id is null''', dbConnection)
    products["customer_id"] = int(customer_id)
    dbConnection.close()    

    # Load SVD model
    with open('models/svd','rb') as file:
        svd = pickle.load(file)
        print('model loaded')

    # Predict score on unseen items
    recommendations = []
    for i, j in products.iterrows():
        result = svd.predict(uid=j["customer_id"],iid=j["product_id"])
        # Consider those items with score >= 3
        if result.est >=3:
            recommendations.append({"Product":j["product_name"], "Category":j["product_category_name"], "Score": result.est})
    
    # Return 5 recommendations
    return jsonify(sorted(recommendations, key = lambda item: item['Score'], reverse=True)[0:5])


if __name__ == "__main__":
    app.run(debug=True)