import sys
from pyspark.ml import Pipeline
from colour_tools import colour_schema, rgb2lab_query, plot_predictions
from pyspark.sql import SparkSession, functions, types
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier, MultilayerPerceptronClassifier, GBTClassifier

spark = SparkSession.builder.appName('colour predicter').getOrCreate()
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+

def main(inputs):
    data = spark.read.csv(inputs, header = True, schema = colour_schema)
    lab_query = rgb2lab_query(passthrough_columns = ['labelword'])

    # TODO: actually build the components for the pipelines, and the pipelines.
    indexer = StringIndexer(inputCol = "labelword", outputCol = "labelCol", handleInvalid = 'error')
    
    rgb_assembler = VectorAssembler(inputCols = ['R', 'G', 'B'], outputCol = "features")
    lab_assembler = VectorAssembler(inputCols = ['lL', 'lA', 'lB'], outputCol = "features") 
    
    forest = RandomForestClassifier(numTrees = 22, maxDepth = 10, labelCol = "labelCol", seed = 42)
    mlp = MultilayerPerceptronClassifier(maxIter = 400, layers = [3, 16, 11], blockSize = 1, seed = 123, labelCol = "labelCol")

    sqlTrans = SQLTransformer(statement = lab_query)
    
    models = [
        ('RGB-forest', Pipeline(stages = [indexer, rgb_assembler, forest])),
        ('LAB-forest', Pipeline(stages = [sqlTrans, indexer, lab_assembler, forest])),
        
        ('RGB-MLP', Pipeline(stages = [indexer, rgb_assembler, mlp])),
        ('LAB-MLP', Pipeline(stages = [sqlTrans, indexer, lab_assembler, mlp])),
    ]

    # TODO: need an evaluator
    evaluator = MulticlassClassificationEvaluator(labelCol = "labelCol" , predictionCol = "prediction")

    # TODO: split data into training and testing
    train, test = data.randomSplit([0.75, 0.25])
    train = train.cache()
    test = test.cache()
    score_dict = dict()
    for label, pipeline in models:
        # TODO: fit the pipeline to create a model
        model = pipeline.fit(train)
        
        # Output a visual representation of the predictions we're
        # making: uncomment when you have a model working
        plot_predictions(model, label)

        # TODO: predict on the test data
        predictions = model.transform(test)
        
        # calculate a score
        score = evaluator.evaluate(predictions)
        score_dict[label] = score
    return score_dict

if __name__ == "__main__":
    inputs = sys.argv[1]
    score_dict = main(inputs)
    for key, value in score_dict.items():
        print(key, value)
