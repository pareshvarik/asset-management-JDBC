package com.dev.exceptions;

@SuppressWarnings("serial")
public class LoginException extends RuntimeException {
public String getMessage()
{
	return "login exception";
	
}
}
