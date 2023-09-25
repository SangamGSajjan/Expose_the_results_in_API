from flask import Flask, g, jsonify, render_template
import sqlite3
from sqlalchemy import create_engine

app = Flask(__name__, template_folder="./")
file_path = "./Dataset.db"
conn = sqlite3.connect(file_path)
cursor = conn.cursor()


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(file_path)
        g.db.row_factory = sqlite3.Row  
    return g.db


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/question_1", methods=["GET"])
def question_1():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT * FROM property_details AS pd JOIN property_price_details AS ppd ON pd.id = ppd.id WHERE ppd.price > 1000000 AND pd.l1 = 'Estados Unidos' ;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_2", methods=["GET"])
def question_2():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT *,CASE WHEN surface_total < 50 THEN 'Small' WHEN surface_total BETWEEN 50 AND 100 THEN 'Medium' ELSE 'Large' END AS surface_category FROM property_details;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_3", methods=["GET"])
def question_3():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT  DISTINCT pd1.id,pd1.l3 FROM Property_Details pd1 JOIN Property_Details pd2 ON pd1.l3 = 'Belgrano' AND pd2.l3 = 'Belgrano' AND pd1.id <> pd2.id AND pd1.bedrooms = pd2.bedrooms AND pd1.bathrooms = pd2.bathrooms;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_4", methods=["GET"])
def question_4():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT ppd.property_type,AVG(ppd.price / pd.surface_total) AS avg_price_per_sq_meter FROM Property_Price_Details ppd JOIN Property_Details pd ON ppd.id = pd.id WHERE pd.l3 = 'Belgrano' GROUP BY ppd.property_type;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_5", methods=["GET"])
def question_5():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT pd.id, ppd.price FROM Property_Price_Details ppd JOIN Property_Details pd ON ppd.id = pd.id WHERE ppd.price > (SELECT AVG(ppd2.price/pd.surface_total) FROM Property_Price_Details ppd2 JOIN Property_Details pd2 ON ppd2.id = pd2.id WHERE pd2.bedrooms = pd.bedrooms AND pd2.bathrooms = pd.bathrooms);"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_6", methods=["GET"])
def question_6():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT ppd.property_type, pd.created_on, SUM(ppd.price) AS cumulative_price FROM property_details pd INNER JOIN property_price_details ppd ON pd.id = ppd.id GROUP BY ppd.property_type, pd.created_on ORDER BY pd.created_on;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_7", methods=["GET"])
def question_7():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT pd.l3, SUM(pd.surface_total) AS total_surface_area FROM property_details pd INNER JOIN property_price_details ppd ON pd.id = ppd.id WHERE ppd.operation_type = 'Venta' GROUP BY pd.l3 ORDER BY total_surface_area DESC LIMIT 10;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_8", methods=["GET"])
def question_8():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT pd.id, ppd.price, pd.created_on FROM property_details pd INNER JOIN property_price_details ppd ON pd.id = ppd.id WHERE pd.l3 = 'Palermo'  AND ppd.operation_type = 'Venta'  AND pd.created_on >= '2020-08-01'  AND pd.created_on <= '2020-08-31' ORDER BY ppd.price DESC LIMIT 5;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_9", methods=["GET"])
def question_9():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "WITH ranked_properties AS (SELECT pd.id, ppd.property_type, ppd.price, pd.surface_total,ROW_NUMBER() OVER(PARTITION BY ppd.property_type ORDER BY ppd.price / pd.surface_total DESC) AS rank  FROM property_details pd  INNER JOIN property_price_details ppd ON pd.id = ppd.id)SELECT rp.id, rp.property_type, rp.price, rp.surface_total, rp.rank FROM ranked_properties rp WHERE rp.rank <= 3;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


@app.route("/question_10", methods=["GET"])
def question_10():
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT l1,  l2,  l3,  AVG(price / surface_total) AS average_price_per_square_meter FROM property_details pd INNER JOIN property_price_details ppd ON pd.id = ppd.id WHERE ppd.operation_type = 'Venta'  AND pd.created_on BETWEEN '2020-01-01' AND '2020-12-31' GROUP BY l1, l2, l3 HAVING COUNT(*) >= 10 ORDER BY AVG(price / surface_total) DESC LIMIT 3;"
    )
    data = cursor.fetchall()
    result = [dict(row) for row in data]
    return jsonify(result)


if __name__ == "__main__":
    app.run()
