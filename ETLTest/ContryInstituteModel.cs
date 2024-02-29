using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using ETLBox;
using ETLBox.DataFlow;

namespace ETLTest
{
    public class CountryInstituteModel
    {
        [ColumnMap("Pages")]
        public string pages { get; set; }
        public string[] web_pages { get; set; }
        [ColumnMap("CountryName")]
        public string country { get; set; }

        [ColumnMap("InstituteName")]
        public string name { get; set; }
        public string[] domains { get; set; }
        public string alpha_two_code { get; set; }
        [JsonPropertyName("state-province")]
        public string state_province { get; set; }

        public CountryInstituteModel Normalize()
        {
            this.country = country.Trim().ToUpper();
            this.name = name.Trim();
            return this;
        }

        public bool IsValid()
        {

            if (string.IsNullOrEmpty(country) || 
                string.IsNullOrEmpty(this.name))
                return false;
            if (country.Length < 5 || country.Length > 50)
                return false;
            if (name.Length < 5 || name.Length > 50)
                return false;
            return true;
        }
    }

    public class CountryInstituteModelEntry
    {
        public CountryInstituteModelEntry()
        {
        }

        public string Pages { get; set; }
        public string CountryName { get; set; }
        public string InstituteName { get; set; }
    }

    public class CountryInstituteCollection : ObservableCollection<CountryInstituteModel>
    {
        public CountryInstituteCollection() { }
    }
}
