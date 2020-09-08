package com.sap.hadoop.client.spark.perfload.logs;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * An iris classification pipeline using a neural network. Derived from
 * {@code org.apache.spark.examples.ml.JavaSimpleTextClassificationPipeline
 * JavaSimpleTextClassificationPipeline}. Run with
 *
 * <pre>
 * bin/run-example ml.JavaIrisClassification
 * </pre>
 */
public class JavaIrisClassification {

    public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\tools");
		String master = "local[*]";
		
		SparkSession session = SparkSession.builder().appName("TestCorrelateAllLogs").master("local").getOrCreate();
		SparkContext sparkContext = session.sparkContext();
		
      //  SparkConf conf = new SparkConf()
        //        .setAppName("Iris Classification Pipeline (Java)");
        //SparkContext jsc = new SparkContext(conf);
        SQLContext jsql = new SQLContext(sparkContext);

        String path = "D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\iris_svmLight_0.txt";
        
        Dataset<Row> data = session.read().format("libsvm").load(path);
        data.show(100);

        System.out.println("\nLoaded IRIS dataframe:");
        data.show(100);

        // prepare train/test set
        Dataset<Row> trainingData = data.sample(false, 0.6, 11L);
        Dataset<Row> testData = data.except(trainingData);

        // Configure an ML pipeline to train a model. In this example,
        // the pipeline combines Spark ML and DL4J elements.
        StandardScaler scaler = new StandardScaler()
                .setWithMean(true).setWithStd(true)
                .setInputCol("features")
                .setOutputCol("scaledFeatures");
      //  NeuralNetworkClassification classification = new NeuralNetworkClassification()
       //         .setFeaturesCol("scaledFeatures")
        //        .setConf(getConfiguration());
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {
                scaler });

        // Fit the pipeline on training data.
        System.out.println("\nTraining...");
        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions on test data.
        System.out.println("\nTesting...");
        Dataset<Row> predictions = model.transform(testData);

        System.out.println("\nTest Results:");
        predictions.show(100);
    }

  /*  private static MultiLayerConfiguration getConfiguration() {

        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(11L) // Seed to lock in weight initialization for tuning
                .iterations(100) // # training iterations predict/classify & backprop
                .learningRate(1e-3f) // Optimization step size
                .optimizationAlgo(OptimizationAlgorithm.LINE_GRADIENT_DESCENT) // Backprop method (calculate the gradients)
                .momentum(0.9)
                .constrainGradientToUnitNorm(true)
                .useDropConnect(true)
                .list(2) // # NN layers (does not count input layer)
                .layer(0, new RBM.Builder(RBM.HiddenUnit.RECTIFIED, RBM.VisibleUnit.GAUSSIAN)
                                .nIn(4) // # input nodes
                                .nOut(3) // # fully connected hidden layer nodes. Add list if multiple layers.
                                .weightInit(WeightInit.XAVIER)
                                .activation("relu")
                                .lossFunction(LossFunctions.LossFunction.RMSE_XENT)
                                .updater(Updater.ADAGRAD)
                                .k(1) // # contrastive divergence iterations
                                .dropOut(0.5)
                                .build()
                ) // NN layer type
                .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
                                .nIn(3) // # input nodes
                                .nOut(3) // # output nodes
                                .activation("softmax")
                                .weightInit(WeightInit.XAVIER)
                                .updater(Updater.ADAGRAD)
                                .dropOut(0.5)
                                .build()
                ) // NN layer type
                .build();

        return conf;
    } */
}
