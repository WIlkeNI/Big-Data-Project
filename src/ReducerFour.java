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
		private String bonus;

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
		public String getBonus(){
			return this.bonus;
		}
		public void setBonus(String val){
			this.bonus = val;
		}
	}

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Text valEmit = new Text();
		String merge = "";

	    HashMap<Integer, String> bonusDepartamento = new HashMap<Integer, String>();
	    //bonusDepartamento.put(0,"0");
	    bonusDepartamento.put(1, "1.35");   bonusDepartamento.put(2, "1.3");
	    bonusDepartamento.put(3, "1.25");   bonusDepartamento.put(4, "1.2");
	    bonusDepartamento.put(5, "1.15");   bonusDepartamento.put(6, "1.1");

		HashMap<Integer, Departamento> ventasDep = new HashMap<Integer, Departamento>();
		List<String> empleados = new ArrayList<String>();
		for (Object val : values) {
			String[] value = val.toString().split("\t");
			if (value.length == 3){
				Departamento d = new Departamento(Integer.parseInt(value[0]), Double.parseDouble(value[2].replace(",",".")));
				ventasDep.put(d.getId(), d);
			}
			else {
				empleados.add(val.toString());
			}
		}

		List<Departamento> dptos = new ArrayList<Departamento>(ventasDep.values());

		Collections.sort(dptos, new Comparator<Departamento>(){

			@Override
			public int compare(Departamento d1, Departamento d2){
				return (int)(d2.getValor() - d1.getValor());
			}
		});

		int i = 1;
		List<String> bonusDepto = new ArrayList<String>();
		for (Departamento d: dptos){
			 //double value = d.getValor();
			 int keyDepartamento = d.getId();
			 //bonusDepto.add(keyDepartamento, bonusDepartamento.get(i));
			 ventasDep.get(keyDepartamento).setBonus(bonusDepartamento.get(i));
			 //context.write(new LongWritable(keyDepartamento), new Text(String.valueOf(value) + "\t" + bonusDepartamento.get(i)));
			 if (i < 6){
				 i++;
			 }

		}
		
		for (String empleado: empleados) {
			String[] campos = empleado.toString().split("\t");
			int idEmpleado = Integer.parseInt(campos[0]);
			String sueldoBasico = campos[1];
			String bonusPersonal = campos[5];
			int idDepartamento = Integer.parseInt(campos[2]);
			if (idDepartamento != 0){
				Departamento d = ventasDep.get(idDepartamento);
				context.write(new LongWritable(idEmpleado), new Text(sueldoBasico + "\t" + bonusPersonal + "\t" + d.getBonus()));
			}
			else {
				context.write(new LongWritable(idEmpleado), new Text(sueldoBasico + "\t" + "1" + "\t" + "1"));
			}
		}
	}

}
