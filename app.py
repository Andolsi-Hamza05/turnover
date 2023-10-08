""" the main application python file -flask- """

from flask import Flask,request,render_template
from src.logger import logging
from src.pipeline.predict_pipeline import CustomData,PredictPipeline

application=Flask(__name__)

app=application

## Route for a home page

@app.route('/')
def index():
    return render_template('index.html') 

@app.route('/predictdata', methods=['GET', 'POST'])
def predict_datapoint():
    """
    Endpoint to predict data based on user input.

    This function handles both GET and POST requests. In a POST request, it expects form data
    with specific fields (position, promotion, assignments, salary, seniority_years, gender, absences).

    Returns:
        str: A message indicating the prediction result or an error message if prediction fails.
    """
    try:
        if request.method == 'GET':
            return render_template('home.html')
        else:
            data = CustomData(
                position=request.form.get('position'),
                promotion=request.form.get('promotion'),
                assignments=request.form.get('assignments'),
                salary=request.form.get('salary'),
                seniority=request.form.get('seniority'),
                gender=request.form.get('gender'),
                absences=request.form.get('absences')
            )
            pred_df = data.get_data_as_data_frame()
            logging.info(pred_df)
            logging.info("Before Prediction")

            predict_pipeline = PredictPipeline()
            logging.info("Mid Prediction")
            results = predict_pipeline.predict(pred_df)
            logging.info("After Prediction")
            return render_template('home.html', results=results[0])
    except Exception as e:
        # Print the error explanation
        error_message = "An error occurred during predict_datapoint in app."
        print(error_message)
        print("Error details:", str(e))
        if e.__cause__:
            print("Main cause:", str(e.__cause__))

        # Log the error
        logging.exception(error_message)

        # You can also customize the error message or response to the user
        return f"{error_message} Please try again later."



if __name__=="__main__":
    app.run(host="0.0.0.0",debug=True)        

# EOF
