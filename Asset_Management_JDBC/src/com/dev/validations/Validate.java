package com.dev.validations;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Validate {

	public Boolean idValidation(String id) {
		Pattern pat = Pattern.compile("\\d+"); 
		Matcher mat = pat.matcher(id);
		if(mat.matches() && (Integer.parseInt(id)>=0)) {

			return true;
		}

		return false;
	}


	public Boolean nameValidation(String name)  {
		Pattern pat = Pattern.compile("[A-Za-z]{1,25}"); 
		Matcher mat = pat.matcher(name);
		if(mat.matches()) {
			return true;
		}
		return false;

	}



	public Boolean PasswordValidation(String password) {
		Pattern pat = Pattern.compile("\\w+\\W+\\w+"); 
		Matcher mat = pat.matcher(password);
		if(mat.matches()) {
			return true;
		}
		return false;


	}


	public Boolean dateValidation(String date)  {
		Pattern pat = Pattern.compile("^([0-2][0-9]|(3)[0-1])(\\/)(((0)[0-9])|((1)[0-2]))(\\/)\\d{4}$"); 
		Matcher mat = pat.matcher(date);
		if(mat.matches()) { 
			return true;
		}
		else
		{
			return false;
		}
	}




}



