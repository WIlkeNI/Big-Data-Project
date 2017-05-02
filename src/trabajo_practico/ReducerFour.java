package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.*;
import org.apache.hadoop.mapreduce.Reducer;



public class ReducerFour extends Reducer<LongWritable, Text, LongWritable, Text/*LongWritable*//*Deberia devolver un text con las columnas concatenadas*/> {

	public class Departamento{

		private int id;
		private double valor;
		private double bonus;

		public Departamento(int i, double v){
			this.id = i;
			this.valor = v;
		}

		public double getValor(){
			return this.valor;
		}
		public int getId(){
			return this.id;
		}
		public double getBonus(){
			return this.bonus;
		}
		public void setBonus(double val){
			this.bonus = val;
		}
	}

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


		Text valEmit = new Text();
		String merge = "";

    HashMap<Integer, String> bonusDepartamento = new HashMap<Integer, String>();
    bonusDepartamento.put(1, "1.35");   bonusDepartamento.put(2, "1.3");
    bonusDepartamento.put(3, "1.25");   bonusDepartamento.put(4, "1.2");
    bonusDepartamento.put(4, "1.15");   bonusDepartamento.put(6, "1.1");

		HashMap<Integer, Departamento> ventasDep = new HashMap<Integer, Departamento>();

    for (@SuppressWarnings("unused") Object val : values) {

			String[] value = val.toString().split("\t");
			Departamento d = new Departamento(Integer.parseInt(value[0]), Double.parseDouble(value[2]));
			ventasDep.put(d.getId(), d);

		}

		List<Departamento> dptos = new ArrayList<Departamento>(ventasDep.values());

		Collections.sort(dptos, new Comparator<Departamento>(){

			@Override
			public int compare(Departamento d1, Departamento d2){
				return (int)(d1.getValor() - d2.getValor());
			}
		});

		int i = 1;
		for (Departamento d: dptos){
			 double value = d.getValor();
       int keyDepartamento = d.getId();
			 context.write(new LongWritable(keyDepartamento), new Text(String.valueOf(value) + "\t" + bonusDepartamento.get(i)));
			 if (i < 6){
				 i++;
			 }

		}

	}

}
