package com.myspark;

import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

public class DataType {
    public static void main(String[] args) {
        
        // ------ Labeled Point ------
        // Create a labeled point with a positive label and a dense feature vector.
        LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));
        
        // Create a labeled point with a negative label and a sparse feature vector.
        LabeledPoint neg = new LabeledPoint(1.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));
        
        
        // ------ Dense Matrix ------
        // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        Matrix dm = Matrices.dense(3, 2, new double[] { 1.0, 3.0, 5.0, 2.0, 4.0, 6.0 }); // first col , second col...
        
        System.out.println(dm.apply(0, 0)); // row:0; col:0
        System.out.println(dm.apply(1, 0)); // row:1; col:0
    }
}
