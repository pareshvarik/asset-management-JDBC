package com.dev.exceptions;

@SuppressWarnings("serial")
public class InvalidIdException extends RuntimeException {
	
	public InvalidIdException(){
		System.out.println("Invalid, Only  positive numbers are allowed");
	}
	
	

}
