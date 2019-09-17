package com.dev.app;

import java.util.Scanner;
import com.dev.beans.Asset;
import com.dev.beans.AssetAllocation;
import com.dev.beans.Employee;
import com.dev.beans.UserMaster;
import com.dev.exceptions.ValidationException;
import com.dev.services.Services;
import com.dev.services.ServicesImpl;
import com.dev.validations.Validate;

public class App {
	public static void main(String[] args) {
		ValidationException validationexception = new ValidationException();
		Scanner input = new Scanner(System.in);
		Validate validate = new Validate();
		Services services = new ServicesImpl();
		main: while (true) {
			System.out.println(
					"************************************ ASSET MANAGEMENT SYSTEM *************************************************");
			System.out.println("1.Manager");
			System.out.println("2.Admin");
			System.out.println("3.Exit aplication");
			System.out.println("Enter your Choice:");

			String homeChoice = input.next();
			while (!validate.idValidation(homeChoice)) {
				try {
					throw validationexception;

				} catch (ValidationException e) {
					System.out.println("Enter your Choice:");
					homeChoice = input.next();
				}
			}
			manager: switch (Integer.parseInt(homeChoice)) {
			case 1:// manager
				while (true) {
					System.out.println(
							"************************************ Manager Portal *************************************************");
					System.out.println("1.login");
					System.out.println("2.Go Back to Home");
					System.out.println("Enter your Choice:");
					String managerChoice = input.next();
					while (!validate.idValidation(managerChoice)) {
						try {
							throw validationexception;
						} catch (ValidationException e) {
							System.out.println("Enter your Choice:");
							managerChoice = input.next();
						}
					}
					System.out.println(
							"*******************************************************************************************");
					loginmanager: switch (Integer.parseInt(managerChoice)) {
					case 1:// login manager
						while (true) {
							// LOGIN manager
							System.out.println("Manager ID:");
							String managerId = input.next();
							while (!validate.idValidation(managerId)) {
								try {
									throw validationexception;
								} catch (ValidationException e) {
									System.out.println("Enter correct Manager ID:");
									managerId = input.next();
								}
							}
							System.out.println("Password:");
							String password = input.next();
							// Check Login Credentials
							UserMaster um = services.loginService(Integer.parseInt(managerId), password);
							if (um.getUsertype() != null) {
								if (um.getUsertype().equalsIgnoreCase("manager")) {
									System.out.println("Login Successful of Manager");
									System.out.println(
											"************************************************ Manager home page *********************************************************");
									while (true) {
										System.out.println("1:Raise the Request");
										System.out.println("2:View the stauts of raised request");
										System.out.println("3:ADD Newly Joined Employee");
										System.out.println("4.LogOut ");
										Integer option = input.nextInt();
										Employee emp = new Employee();
										System.out.println("Enter your Choice:");

										switch (option) {
										case 1: // Raised request
											AssetAllocation setObj = new AssetAllocation();
											Integer min = 1;
											Integer max = 300;
											Integer allocationId = (int) ((Math.random() * ((max - min) + 1)) + min);
											setObj.setAllocationid(allocationId);
											System.out.println("Enter Asset Id");
											String assetid = input.next();
											Boolean b6 = validate.idValidation(assetid);
											jump: while (!b6) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													assetid = input.next();
													if (validate.idValidation(assetid)) {
														break jump;
													}
												}
											}
											setObj.setAssetid(Integer.parseInt(assetid));
											System.out.println("Enter Employeee Number");
											String empno = input.next();
											Boolean b7 = validate.idValidation(empno);
											jump: while (!b7) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													empno = input.next();
													if (validate.idValidation(empno)) {
														break jump;
													}
												}
											}
											setObj.setEmpno(Integer.parseInt(empno));

											System.out.println("Enter aloocation date");
											String date = input.next();
											Boolean b8 = validate.dateValidation(date);
											jump: while (!b8) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter in YYYY/MM/DD format");
													System.out.println("enter again");
													date = input.next();
													if (!validate.dateValidation(date)) {
														break jump;
													}
												}
											}
											setObj.setAllocationdate(date);
											System.out.println("Enter release date");
											String reldate = input.next();
											Boolean b9 = validate.dateValidation(reldate);
											jump: while (!b9) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter in YYYY/MM/DD format");
													System.out.println("enter again");
													reldate = input.next();
													if (!validate.dateValidation(reldate)) {
														break jump;
													}
												}
											}
											setObj.setReleasedate(reldate);

											System.out.println("Enter Quantity");
											String quantity = input.next();

											Boolean b10 = validate.idValidation(quantity);
											jump: while (!b10) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													quantity = input.next();
													if (validate.idValidation(quantity)) {
														break jump;
													}
												}
											}

											setObj.setQuantity(Integer.parseInt(quantity));
											System.out.println(
													"Request raised : " + services.raiseAllocationService(setObj));
											System.out.println("Allocation id :" + setObj.getAllocationid());
											break;
										case 2:// view the status
											System.out.println("enter the allocation id");
											String allocationid = input.next();

											Boolean b11 = validate.idValidation(allocationid);
											jump: while (!b11) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													allocationid = input.next();
													if (validate.idValidation(allocationid)) {
														break jump;
													}
												}
											}
											System.out.println(
													services.viewStatusService(Integer.parseInt(allocationid)));
											break;

										case 3:// add new employee
											System.out.println("Enter Employee number");
											String empid = input.next();

											Boolean b = validate.idValidation(empid);
											jump: while (!b) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													empid = input.next();
													if (validate.idValidation(empid)) {
														break jump;
													}
												}
											}
											emp.setEmpno(Integer.parseInt(empid));
											System.out.println("Employee name");
											String name = input.next();
											Boolean b1 = validate.idValidation(name);
											jumpvalidate: while (b1) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter in string format");
													System.out.println("enter again");
													name = input.next();
													if (!validate.idValidation(name)) {
														break jumpvalidate;
													}
												}
											}
											emp.setEname(name);

											System.out.println("Employee Hiredate");
											String hiredate = input.next();
											Boolean b3 = validate.dateValidation(hiredate);
											jump: while (!b3) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter in YYYY/MM/DD format");
													System.out.println("enter again");
													hiredate = input.next();
													if (!validate.idValidation(hiredate)) {
														break jump;
													}
												}
											}
											emp.setHiredate(hiredate);

											System.out.println("Enter Job of Employee");
											String job = input.next();
											Boolean b4 = validate.idValidation(job);
											jumpvalidate: while (b4) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter in string format");
													System.out.println("enter again");
													job = input.next();
													if (!validate.idValidation(job)) {
														break jumpvalidate;
													}
												}
											}
											emp.setJob(job);

											System.out.println("Manager Number");
											String mgr = input.next();

											Boolean b5 = validate.idValidation(mgr);
											jump: while (!b5) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													mgr = input.next();
													if (validate.idValidation(mgr)) {
														break jump;
													}
												}
											}
											emp.setMgrno(Integer.parseInt(mgr));

											System.out.println("Department ID");
											String deptid = input.next();

											Boolean b2 = validate.idValidation(deptid);
											jump: while (!b2) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													deptid = input.next();
													if (validate.idValidation(deptid)) {
														break jump;
													}
												}
											}
											emp.setDeptid(Integer.parseInt(deptid));

											System.out.println("Added Employee: " + services.addEmployeeService(emp));
											break;
										case 4:// logout
											break manager;
										default:
											System.out.println("Invalid option");
											break manager;
										}// manager switch
									} // manager home page
								} // if condition of manager
							} else {
								System.out.println("Invalid user name or password");
								break loginmanager;
							}

						} // manager login
					case 2: // go back
						break manager;
					}// manager switch
				} // manager while loop

			case 2:// Admin

				while (true) {
					System.out.println(
							"************************************ Admin Portal *************************************************");
					System.out.println("1.Login");
					System.out.println("2.Go Back to Home");
					System.out.println("Enter your Choice:");
					String managerChoice = input.next();
					while (!validate.idValidation(managerChoice)) {
						try {
							throw validationexception;
						} catch (ValidationException e) {
							System.out.println("Enter your Choice:");
							managerChoice = input.next();
						}
					}
					System.out.println(
							"**************************************************************************************************");
					loginadmin: switch (Integer.parseInt(managerChoice)) {
					case 1:// login Admin
					{
						admin: while (true) {
							// LOGIN Admin
							System.out.println("Admin ID:");
							String managerId = input.next();
							while (!validate.idValidation(managerId)) {
								try {
									throw validationexception;

								} catch (ValidationException e) {
									System.out.println("Enter correct Admin ID:");
									managerId = input.next();
								}
							}

							System.out.println("Password:");
							String password = input.next();

							// Check Login Credentials
							UserMaster um = services.loginService(Integer.parseInt(managerId), password);
							if (um.getUsertype() != null) {
								if (um.getUsertype().equalsIgnoreCase("admin")) {
									System.out.println("Login Successful of admin");
									System.out.println(
											"************************************************ LOGIN of Admin *********************************************************");
									while (true) {
										System.out.println("enter your choice");
										System.out.println("1. Add asset");
										System.out.println("2. Remove asset");
										System.out.println("3. Update asset");
										System.out.println("4. View all asset");
										System.out.println("5. View all allocation request");
										System.out.println("6. Set allocation status");
										System.out.println("7. logout");
										Integer choice = input.nextInt();
										Asset as = new Asset();
										switch (choice) {
										case 1:

											System.out.println("Enter Asset id");
											String assid = input.next();

											Boolean b = validate.idValidation(assid);
											jumpadmin: while (!b) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													assid = input.next();
													if (validate.idValidation(assid)) {
														break jumpadmin;
													}
												}
											}
											as.setAssetid(Integer.parseInt(assid));
											System.out.println("Asset name");
											String assname = input.next();
											Boolean b1 = validate.idValidation(assname);
											jumpadmin: while (b1) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter in string format");
													System.out.println("enter again");
													assname = input.next();
													if (!validate.idValidation(assname)) {
														break jumpadmin;
													}
												}
											}
											as.setAssetname(assname);
											System.out.println("Asset des");
											as.setAssetdes(input.next());
											System.out.println("Enter the Quantity");
											String assetquantity = input.next();

											Boolean b2 = validate.idValidation(assetquantity);
											jumpadmin: while (!b2) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													assetquantity = input.next();
													if (validate.idValidation(assetquantity)) {
														break jumpadmin;
													}
												}
											}
											as.setQuantity(Integer.parseInt(assetquantity));
											System.out.println("Status");
											String status = input.next();
											Boolean b3 = validate.idValidation(status);
											jumpadmin: while (b3) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter in string format");
													System.out.println("enter again");
													status = input.next();
													if (!validate.idValidation(status)) {
														break jumpadmin;
													}
												}
											}
											as.setStatus(status);
											System.out.println("Added asset :" + services.addAssetService(as));
											break;

										case 2:

											System.out.println("ENTER ASSET ID YOU WANT TO REMOVE");
											Asset a1 = new Asset();
											String assid1 = input.next();

											Boolean b4 = validate.idValidation(assid1);
											jumpadmin: while (!b4) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													assid1 = input.next();
													if (validate.idValidation(assid1)) {
														break jumpadmin;
													}
												}
											}
											a1 = services.removeAssetService(Integer.parseInt(assid1));
											System.out.println("removed asset is :" + a1);
											break;
										case 3:
											System.out.println("enter the asset id you want to update");
											String assid2 = input.next();

											Boolean b5 = validate.idValidation(assid2);
											jumpadmin: while (!b5) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													assid2 = input.next();
													if (validate.idValidation(assid2)) {
														break jumpadmin;
													}
												}
											}

											System.out.println("to update asset");
											System.out.println("enter the choice");
											System.out.println("1.update asset name");
											System.out.println("2.update asset des ");
											System.out.println("3. update asset quantity");
											System.out.println("4.update asset status");
											String assetchoice = input.next();

											Boolean b10 = validate.idValidation(assetchoice);
											jumpadmin: while (!b10) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													assetchoice = input.next();
													if (validate.idValidation(assetchoice)) {
														break jumpadmin;
													}
												}
											}
											switch (Integer.parseInt(assetchoice)) {
											case 1:
												System.out.println("enter updated asset name");
												String updatedassetname = input.next();
												System.out.println(
														"updated asset name :" + services.updateAssetNameService(
																Integer.parseInt(assid2), updatedassetname));
												break;

											case 2:
												System.out.println("enter updated asset des");
												String updatedassetdes = input.next();
												System.out
														.println("updated asset des :" + services.updateAssetDesService(
																Integer.parseInt(assid2), updatedassetdes));
												break;

											case 3:
												System.out.println("enter updated asset quantity");
												Integer updatedassetquantity = input.nextInt();
												System.out.println("updated asset quantity :"
														+ services.updateAssetQuantityService(Integer.parseInt(assid2),
																updatedassetquantity));
												break;

											case 4:
												System.out.println("enter updated asset status");
												String updatedassetstatus = input.next();
												System.out.println(
														"updated asset status :" + services.updateAssetStatusService(
																Integer.parseInt(assid2), updatedassetstatus));
												break;

											}
											break;

										case 4:
											System.out.println("assets are");
											services.getAllAssetService();
											break;
										case 5:
											services.getAllAssetAllocationService();

											break;
										case 6:
											System.out.println("enter allocation id to set status");
											String allocationid = input.next();

											Boolean b6 = validate.idValidation(allocationid);
											jumpadmin: while (!b6) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter number");
													System.out.println("enter again");
													allocationid = input.next();
													if (validate.idValidation(allocationid)) {
														break jumpadmin;
													}
												}
											}
											Integer allocation_id = Integer.parseInt(allocationid);
											System.out.println("enter status");
											String assetstatus = input.next();
											Boolean b11 = validate.idValidation(assetstatus);
											jumpadmin: while (b11) {
												try {
													throw validationexception;
												} catch (ValidationException e1) {
													System.out.println("please enter in string format");
													System.out.println("enter again");
													assetstatus = input.next();
													if (!validate.idValidation(assetstatus)) {
														break jumpadmin;
													}
												}
											}

											if (services.setStatusService(allocation_id, assetstatus)) {
												System.out.println("status changed");
											} else {
												System.out.println("status not changed");
											}
											break;

										case 7:
											System.out.println("admin logged out successfully");
											break admin;
										default:
											System.out.println("enter valid number");
											break;
										}// switch while loop
									} // Admin while loop
								} // if loop of checking the Admin
							} else {
								System.out.println("Invalid Username or Password");
								break loginadmin;
							}
						} // while loop of admin
					} // case 1 end
					case 2:// go back
					{
						break manager;
					}
					}
				} // while loop of case 2
			case 3:// exit the application
				System.out.println("Thank you visit again");
				break main;
			}// main case
		} // main while loop
		input.close();
	}// main method end
}// class end
