package com.prd.run;

import org.testng.annotations.Test;

import com.prd.data.StoreStatusBase;
import com.prd.data.*;

public class DailyOrdersMonitoring {
	//test
	
	static com.prd.data.StoreStatusBase ssb = new com.prd.data.StoreStatusBase();
	static CheckAvailabilityTable cat = new CheckAvailabilityTable();
	
	
	@Test
	public static void CheckStoreStatus() throws InterruptedException
	
	{
		StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_sunflower, "Sunflower");
		StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_Hapisgah_Steakhouse_PRD ,"Hapisgah");
		StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_FishandFire, "Fish_and_Fire");
		//StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_Boncheur_OLO, "Boncheur");
		StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_ALDOS_PRD_OLO, "Aldos");
		//StoreStatusBase.StoreStatus2(CheckAvailabilityTable.url_BB_Jacks_OLO, "BBJACKS");
		//StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_Billis_Bar_OLO, "Billis");
		//StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_cnpizza, "Country_Pizza");
		StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_CosMos_PRD_OLO, "COSMOS");
		//StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_Weather_Gage_OLO, "Weather_Gage");
		StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_Ventura_PRD_OLO, "Ventura");
		//StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_ShoreBreakPRD, "ShoreBreak_PRD");
		StoreStatusBase.StoreStatus2(CheckAvailabilityTable.url_romapizza, "Roma Pizza");
		StoreStatusBase.StoreStatus(CheckAvailabilityTable.url_Robertos_PRD_OLO, "Robertos");
		StoreStatusBase.StoreStatus2(CheckAvailabilityTable.url_wardroom,"Wardroom");
		
	}
	


}
