package com.revature.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

import com.revature.map.UnemploymentMapper;
import com.revature.reduce.UnemploymentReducer;
import com.revature.writable.DoubleArrayWritable;

public class UnemploymentTest {
	
	private MapDriver<LongWritable, Text, Text, DoubleArrayWritable> mapDriver;
	private ReduceDriver<Text, DoubleArrayWritable, NullWritable, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleArrayWritable, NullWritable, Text> mapReduceDriver;
	
	@Before
	public void setup() {
		UnemploymentMapper mapper = new UnemploymentMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		UnemploymentReducer reducer = new UnemploymentReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	/*
	 * The UnemploymentMapper class cannot be tested due to hashCode output
	 * 
	 * SL.UEM.ADVN.FE.ZS	com.revature.writable.DoubleArrayWritable@77307458
	 * SL.UEM.ADVN.MA.ZS	com.revature.writable.DoubleArrayWritable@1fc0053e
	 * SL.UEM.TOTL.FE.NE.ZS	com.revature.writable.DoubleArrayWritable@290b1b2e
	 * SL.UEM.TOTL.MA.NE.ZS	com.revature.writable.DoubleArrayWritable@47874b25
	 */
//	@Test
//	public void testMapper() throws IOException {
//		
//		DoubleArrayWritable femaleAdvancedUnemployment = new DoubleArrayWritable();
//		DoubleArrayWritable maleAdvancedUnemployment = new DoubleArrayWritable();
//		DoubleArrayWritable femaleUnemployment = new DoubleArrayWritable();
//		DoubleArrayWritable maleUnemployment = new DoubleArrayWritable();
//		
//		DoubleWritable[] femaleAdvancedUnemploymentData = new DoubleWritable[32];
//		DoubleWritable[] maleAdvancedUnemploymentData = new DoubleWritable[32];
//		DoubleWritable[] femaleUnemploymentData = new DoubleWritable[32];
//		DoubleWritable[] maleUnemploymentData = new DoubleWritable[32];
//		
//		double[] femaleAdvancedUnemploymentInput = {2.599999905,2.950000048,2.680000067,2.359999895,2.059999943,2.559999943,3.019999981,1.600000024,1.570000052,3.75,3.480000019,3.109999895,2.720000029,2.460000038,2.279999971,2.170000076,2.609999895,3.359999895,3.349999905,3.140000105,2.900000095,2.660000086,2.529999971,3.119999886,2.319999933,2.410000086,2.25999999,2.200000048,1.980000019,1.74000001,1.419999957,1.350000024};
//		double[] maleAdvancedUnemploymentInput = {3.519999981,2.99000001,2.839999914,2.410000086,3,2.700000048,3.910000086,1.720000029,1.539999962,2.920000076,2.720000029,2.569999933,2.230000019,1.919999957,1.950000048,1.75,2.359999895,3.25,3.519999981,2.950000048,2.5,2.180000067,2.210000038,2.910000086,2.470000029,2.49000001,2.25999999,2,1.899999976,1.620000005,1.309999943,1.289999962};
//		double[] femaleUnemploymentInput = {7.429999828,7.070000172,6.190000057,5.559999943,5.360000134,5.53000021,6.440000057,7.03000021,6.610000134,6.019999981,5.610000134,5.429999828,5.019999981,4.619999886,4.340000153,4.099999905,4.650000095,5.610000134,5.670000076,5.400000095,5.099999905,4.630000114,4.5,5.420000076,8.069999695,8.619999886,8.460000038,7.889999866,7.079999924,6.059999943,5.179999828,4.789999962};
//		double[] maleUnemploymentInput = {7.019999981,6.920000076,6.190000057,5.460000038,5.199999809,5.659999847,7.150000095,7.889999866,7.179999828,6.170000076,5.579999924,5.380000114,4.880000114,4.420000076,4.110000134,3.900000095,4.800000191,5.929999828,6.269999981,5.639999866,5.070000172,4.619999886,4.730000019,6.099999905,10.28999996,10.52000046,9.369999886,8.220000267,7.639999866,6.260000229,5.369999886,4.940000057};
//		
//		for(int i = 0; i < 32; i++) {
//			femaleAdvancedUnemploymentData[i] = new DoubleWritable(femaleAdvancedUnemploymentInput[i]);
//			maleAdvancedUnemploymentData[i] = new DoubleWritable(maleAdvancedUnemploymentInput[i]);
//			femaleUnemploymentData[i] = new DoubleWritable(femaleUnemploymentInput[i]);
//			maleUnemploymentData[i] = new DoubleWritable(maleUnemploymentInput[i]);
//		}
//		
//		femaleAdvancedUnemployment.set(femaleAdvancedUnemploymentData);
//		maleAdvancedUnemployment.set(maleAdvancedUnemploymentData);
//		femaleUnemployment.set(femaleUnemploymentData);
//		maleUnemployment.set(maleUnemploymentData);
//		
//		mapDriver.withInput(new Pair<LongWritable, Text>(new LongWritable(1), new Text("United States,USA,\"Unemployment with advanced education, female (% of female labor force with advanced education)\",SL.UEM.ADVN.FE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,2.599999905,2.950000048,2.680000067,2.359999895,2.059999943,2.559999943,3.019999981,1.600000024,1.570000052,3.75,3.480000019,3.109999895,2.720000029,2.460000038,2.279999971,2.170000076,2.609999895,3.359999895,3.349999905,3.140000105,2.900000095,2.660000086,2.529999971,3.119999886,2.319999933,2.410000086,2.25999999,2.200000048,1.980000019,1.74000001,1.419999957,1.350000024")));
//		mapDriver.withInput(new Pair<LongWritable, Text>(new LongWritable(2), new Text("United States,USA,\"Unemployment with advanced education, male (% of male labor force with advanced education)\",SL.UEM.ADVN.MA.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,3.519999981,2.99000001,2.839999914,2.410000086,3,2.700000048,3.910000086,1.720000029,1.539999962,2.920000076,2.720000029,2.569999933,2.230000019,1.919999957,1.950000048,1.75,2.359999895,3.25,3.519999981,2.950000048,2.5,2.180000067,2.210000038,2.910000086,2.470000029,2.49000001,2.25999999,2,1.899999976,1.620000005,1.309999943,1.289999962")));
//		mapDriver.withInput(new Pair<LongWritable, Text>(new LongWritable(3), new Text("United States,USA,\"Unemployment, female (% of female labor force) (national estimate)\",SL.UEM.TOTL.FE.NE.ZS,5.880000114,7.210000038,6.199999809,6.46999979,6.21999979,5.539999962,4.849999905,5.179999828,4.78000021,4.679999828,5.880000114,6.920000076,6.639999866,6,6.739999771,9.300000191,8.640000343,8.180000305,7.179999828,6.820000172,7.409999847,7.920000076,9.420000076,9.18999958,7.630000114,7.429999828,7.070000172,6.190000057,5.559999943,5.360000134,5.53000021,6.440000057,7.03000021,6.610000134,6.019999981,5.610000134,5.429999828,5.019999981,4.619999886,4.340000153,4.099999905,4.650000095,5.610000134,5.670000076,5.400000095,5.099999905,4.630000114,4.5,5.420000076,8.069999695,8.619999886,8.460000038,7.889999866,7.079999924,6.059999943,5.179999828,4.789999962")));
//		mapDriver.withInput(new Pair<LongWritable, Text>(new LongWritable(4), new Text("United States,USA,\"Unemployment, male (% of male labor force) (national estimate)\",SL.UEM.TOTL.MA.NE.ZS,5.360000134,6.420000076,5.199999809,5.25,4.619999886,3.970000029,3.200000048,3.079999924,2.859999895,2.789999962,4.369999886,5.340000153,4.960000038,4.159999847,4.869999886,7.889999866,7.059999943,6.28000021,5.269999981,5.139999866,6.940000057,7.389999866,9.890000343,9.930000305,7.429999828,7.019999981,6.920000076,6.190000057,5.460000038,5.199999809,5.659999847,7.150000095,7.889999866,7.179999828,6.170000076,5.579999924,5.380000114,4.880000114,4.420000076,4.110000134,3.900000095,4.800000191,5.929999828,6.269999981,5.639999866,5.070000172,4.619999886,4.730000019,6.099999905,10.28999996,10.52000046,9.369999886,8.220000267,7.639999866,6.260000229,5.369999886,4.940000057")));
//		
//		mapDriver.withOutput(new Pair<Text, DoubleArrayWritable>(new Text("SL.UEM.ADVN.FE.ZS"), femaleAdvancedUnemployment));
//		mapDriver.withOutput(new Pair<Text, DoubleArrayWritable>(new Text("SL.UEM.ADVN.MA.ZS"), maleAdvancedUnemployment));
//		mapDriver.withOutput(new Pair<Text, DoubleArrayWritable>(new Text("SL.UEM.TOTL.FE.NE.ZS"), femaleUnemployment));
//		mapDriver.withOutput(new Pair<Text, DoubleArrayWritable>(new Text("SL.UEM.TOTL.MA.NE.ZS"), maleUnemployment));
//
//		mapDriver.runTest();
//	}
	
//	@Test
//	public void testReducer() throws IOException {
//	}
	
//	@Test
//	public void testMapReduce() throws IOException {
}
