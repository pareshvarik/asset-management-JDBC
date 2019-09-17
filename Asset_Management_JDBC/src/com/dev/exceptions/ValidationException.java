package com.dev.exceptions;

@SuppressWarnings("serial")
public class ValidationException extends RuntimeException {
	public String getMessage()
	{
		return "validation exception";

	}
}
