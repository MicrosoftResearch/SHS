/*
 * SHS -- The Scalable Hyperlink Store 
 * 
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

namespace SHS {
  public class HostUtils {
    /* IsNumeric(host) returns true
     * IFF host is of the for quad1.quad2.quad3.quad4
     * AND each quad consists only of the digits 0 through 9
     * AND no quad has a leading 0
     * AND the numeric value of each quad is in the range 0 to 255
     */
    public static bool IsNumeric(string host) {
      int arcCnt = 0;
      int pos = 0;
      for (;;) {
        int quadVal = 0;
        int quadStart = pos;
        while (pos < host.Length && host[pos] != '.') {
          if (host[pos] < '0' || host[pos] > '9') return false;
          if (host[quadStart] == '0' && pos > quadStart) return false;
          quadVal = quadVal * 10 + (host[pos] - '0');
          if (quadVal > 255) return false;
          pos++;
        }
        arcCnt++;
        if (pos == host.Length) return (arcCnt == 4);
        pos++;
      }
    }

    public static string DomainOf(string host) {
      if (HostUtils.IsNumeric(host)) return host;
      int n = host.Length;
      for (int i = 0; i < suffix.Length; i++) {
        if (host.EndsWith(suffix[i])) {
          int p = LastDotBefore(host, n - suffix[i].Length);
          if (p == -1) return host;
          return host.Substring(p+1);
        }
      }
      int q = LastDotBefore(host, n);
      if (q == -1) return host;
      int r = LastDotBefore(host, q);
      if (r == -1) return host;
      return host.Substring(r+1);
    }

    public static string InvertHost(string host) {
      if (HostUtils.IsNumeric(host)) {
        return host;
      } else {
        int n = host.Length;
        char[] res = new char[n];
        int a = 0;
        while (a < n) {
          int c = a;
          while (c < n && host[c] != '.') c++;
          int d = n - c;
          while (a < c) res[d++] = host[a++];
          res[d++] = '.';
          a = c + 1;
        }
        return new string(res);
      }
    }

    private static int LastDotBefore(string s, int n) {
      for (;;) {
        n--;
        if (n < 0 || s[n] == '.') break;
      }
      return n;
    }

    private static readonly string[] suffix = {
      // Non-national TLDs
      ".com",    // 1960296
      ".org",    // 292282
      ".net",    // 265281
      ".edu",    // 52999
      ".info",   // 24816
      ".biz",    // 18388
      ".gov",    // 8471
      ".mil",    // 1465
      ".aero",   // 659
      ".int",    // 548
      ".coop",   // 408
      ".name",   // 345
      ".museum", // 74

      // National TLDs
      ".gov.ac", ".org.ac",
      ".ac", // 363     Ascension Island
      ".ad", // 70      Andorra
      ".ac.ae", ".co.ae", ".gov.ae", ".net.ae", ".org.ae", ".sch.ae",
      ".ae", // 378     United Arab Emirates
      ".com.af", ".gov.af", ".org.af",
      ".af", // 13      Afghanistan
      ".com.ag", ".gov.ag", ".net.ag",
      ".ag", // 327     Antigua and Barbuda
      ".com.ai", ".gov.ai", ".net.ai",
      ".ai", // 98      Anguilla
      ".com.al", ".edu.al", ".gov.al", ".org.al",
      ".al", // 68      Albania
      ".de.am", ".edu.am", ".gov.am",
      ".am", // 486     Armenia
      ".gov.an",
      ".an", // 42      Netherlands Antilles
      ".ao", // 16      Angola
      ".aq", // 5       Antarctica
      ".com.ar", ".edu.ar", ".gov.ar", ".net.ar", ".org.ar",
      ".ar", // 7187    Argentina
      ".as", // 343     American Samoa
      ".ac.at", ".co.at", ".or.at",
      ".at", // 22765   Austria
      ".com.au", ".edu.au", ".gov.au", ".net.au", ".org.au",
      ".au", // 40124   Australia
      ".aw", // 17      Aruba
      ".com.az", ".edu.az", ".gov.az", ".net.az", ".org.az",
      ".az", // 148     Azerbaijan
      ".ax", //         Aland Islands
      ".com.ba", ".edu.ba", ".gov.ba", ".net.ba", ".org.ba",
      ".ba", // 441     Bosnia and Herzegovina
      ".com.bb", ".edu.bb", ".gov.bb", ".org.bb",
      ".bb", // 49      Barbados
      ".ac.bd", ".com.bd", ".edu.bd", ".gov.bd", ".net.bd", ".org.bd",
      ".bd", // 11      Bangladesh
      ".ac.be",
      ".be", // 23047   Belgium
      ".gov.bf",
      ".bf", // 48      Burkina Faso
      ".bg", // 1903    Bulgaria
      ".com.bh", ".edu.bh", ".gov.bh", ".org.bh",
      ".bh", // 53      Bahrain
      ".bi", // 6       Burundi
      ".bj", // 6       Benin
      ".edu.bm", ".gov.bm",
      ".bm", // 166     Bermuda
      ".com.bn", ".edu.bn", ".gov.bn", ".net.bn", ".org.bn",
      ".bn", // 65      Brunei Darussalam
      ".com.bo", ".edu.bo", ".gov.bo", ".net.bo", ".org.bo",
      ".bo", // 162     Bolivia
      ".com.br", ".edu.br", ".gov.br", ".net.br", ".org.br",
      ".br", // 23087   Brazil
      ".com.bs", ".edu.bs", ".gov.bs", ".net.bs", ".org.bs",
      ".bs", // 19      Bahamas
      ".com.bt", ".edu.bt", ".gov.bt", ".net.bt", ".org.bt",
      ".bt", // 40      Bhutan
      ".bv", // 0       Bouvet Island
      ".gov.bw", ".org.bw",
      ".bw", // 47      Botswana
      ".com.by", ".edu.by", ".gov.by", ".net.by", ".org.by",
      ".by", // 1411    Belarus
      ".com.bz", ".edu.bz", ".gov.bz", ".net.bz", ".org.bz",
      ".bz", // 1342    Belize
      ".ab.ca", ".bc.ca", ".mb.ca", ".nb.ca", ".nl.ca", ".ns.ca", ".nt.ca",
      ".nu.ca", ".on.ca", ".pe.ca", ".qc.ca", ".sk.ca", ".yk.ca",
      ".ca", // 41336   Canada
      ".cc", // 5874    Cocos (Keeling) Islands
      ".cd", // 1134    Congo, The Democratic Republic of the
      ".cf", // 1       Central African Republic
      ".cg", // 3       Congo, Republic of
      ".com.ch", ".edu.ch", ".gov.ch",
      ".ch", // 38080   Switzerland
      ".gov.ci",
      ".ci", // 26      Cote d'Ivoire
      ".gov.ck", ".net.ck",
      ".ck", // 45      Cook Islands
      ".gob.cl", ".gov.cl",
      ".cl", // 2688    Chile
      ".gov.cm",
      ".cm", // 13      Cameroon
      ".com.cn", ".edu.cn", ".gov.cn", ".net.cn", ".org.cn",
      ".cn", // 40598   China
      ".com.co", ".edu.co", ".gov.co", ".net.co", ".org.co",
      ".co", // 1118    Colombia
      ".ac.cr", ".co.cr", ".or.cr",
      ".cr", // 326     Costa Rica
      ".cs", // 0       Serbia and Montenegro
      ".com.cu", ".edu.cu", ".gov.cu", ".org.cu",
      ".cu", // 441     Cuba
      ".cv", // 16      Cape Verde
      ".cx", // 1017    Christmas Island
      ".ac.cy", ".com.cy", ".gov.cy", ".net.cy", ".org.cy",
      ".cy", // 404     Cyprus
      ".co.cz", ".or.cz", ".com.cz", ".gov.cz",
      ".cz", // 29306   Czech Republic
      ".de", // 301421  Germany
      ".dj", // 79      Djibouti
      ".ac.dk", ".co.dk", ".gov.dk",
      ".dk", // 25682   Denmark
      ".dm", // 17      Dominica
      ".com.do", ".edu.do", ".gov.do", ".net.do", ".org.do",
      ".do", // 168     Dominican Republic
      ".com.dz", ".edu.dz", ".gov.dz", ".net.dz", ".org.dz",
      ".dz", // 47      Algeria
      ".com.ec", ".edu.ec", ".gov.ec", ".net.ec", ".org.ec",
      ".ec", // 299     Ecuador
      ".com.ee", ".edu.ee", ".gov.ee", ".org.ee",
      ".ee", // 5972    Estonia
      ".com.eg", ".edu.eg", ".gov.eg", ".net.eg", ".org.eg",
      ".eg", // 291     Egypt
      ".eh", // 0       Western Sahara
      ".er", // 1       Eritrea
      ".com.es", ".org.es",
      ".es", // 8191    Spain
      ".com.et", ".edu.et", ".gov.et", ".net.et",
      ".et", // 10      Ethiopia
      ".eu", // 0       European Union
      ".fi", // 11090   Finland
      ".ac.fj", ".com.fj", ".gov.fj", ".org.fj",
      ".fj", // 97      Fiji
      ".gov.fk", ".org.fk",
      ".fk", // 5       Falkland Islands (Malvinas)
      ".ac.fm", ".co.fm",
      ".fm", // 1007    Micronesia, Federal State of
      ".fo", // 182     Faroe Islands
      ".fr", // 35605   France
      ".ga", // 5       Gabon
      ".gb", // 0       Great Britain
      ".gd", // 4       Grenada
      ".com.ge", ".edu.ge", ".gov.ge", ".net.ge", ".org.ge",
      ".ge", // 414     Georgia
      ".gf", // 10      French Guiana
      ".co.gg", ".edu.gg", ".gov.gg", ".org.gg",
      ".gg", // 187     Guernsey
      ".com.gh", ".edu.gh", ".gov.gh", ".org.gh",
      ".gh", // 41      Ghana
      ".com.gi", ".gov.gi",
      ".gi", // 59      Gibraltar
      ".gl", // 142     Greenland
      ".gm", // 22      Gambia
      ".gov.gn", ".net.gn",
      ".gn", // 3       Guinea
      ".gp", // 23      Guadeloupe
      ".gq", // 1       Equatorial Guinea
      ".com.gr", ".edu.gr", ".gov.gr", ".net.gr", ".org.gr",
      ".gr", // 6176    Greece
      ".gs", // 353     South Georgia and the South Sandwich Islands
      ".com.gt", ".edu.gt", ".gob.gt", ".net.gt", ".org.gt",
      ".gt", // 139     Guatemala
      ".com.gu", ".edu.gu", ".gov.gu", ".org.gu",
      ".gu", // 16      Guam
      ".gw", // 0       Guinea-Bissau
      ".edu.gy", ".gov.gy", ".org.gy",
      ".gy", // 29      Guyana
      ".com.hk", ".edu.hk", ".gov.hk", ".net.hk", ".org.hk",
      ".hk", // 2698    Hong Kong
      ".hm", // 99      Heard and McDonald Islands
      ".com.hn", ".edu.hn", ".gob.hn", ".gov.hn", ".org.hn",
      ".hn", // 101     Honduras
      ".com.hr", ".net.hr",
      ".hr", // 2930    Croatia/Hrvatska
      ".ht", // 0       Haiti
      ".info.hu", ".edu.hu", ".gov.hu", ".net.hu", ".org.hu",
      ".hu", // 14280   Hungary
      ".ac.id", ".co.id", ".net.id", ".or.id",
      ".id", // 996     Indonesia
      ".edu.ie", ".gov.ie",
      ".ie", // 4949    Ireland
      ".ac.il", ".co.il", ".gov.il", ".net.il", ".org.il",
      ".il", // 4734    Israel
      ".ac.im", ".co.im", ".gov.im", ".org.im",
      ".im", // 67      Isle of Man
      ".ac.in", ".co.in", ".gov.in", ".net.in", ".org.in",
      ".in", // 1274    India
      ".io", // 10      British Indian Ocean Territory
      ".iq", // 0       Iraq
      ".ac.ir", ".co.ir", ".gov.ir", ".net.ir", ".or.ir", ".org.ir",
      ".ir", // 498     Iran, Islamic Republic of
      ".co.is", ".or.is",
      ".is", // 3283    Iceland
      ".co.it", ".gov.it", ".or.it",
      ".it", // 57664   Italy
      ".ac.je", ".co.je", ".gov.je", ".net.je", ".org.je",
      ".je", // 66      Jersey
      ".com.jm", ".edu.jm", ".gov.jm", ".org.jm",
      ".jm", // 94      Jamaica
      ".com.jo", ".edu.jo", ".gov.jo", ".org.jo",
      ".jo", // 257     Jordan
      ".ac.jp", ".co.jp", ".go.jp", ".ne.jp", ".or.jp",
      ".jp", // 120248  Japan
      ".ac.ke", ".co.ke", ".or.ke",
      ".ke", // 94      Kenya
      ".com.kg", ".edu.kg", ".gov.kg", ".net.kg", ".org.kg",
      ".kg", // 369     Kyrgyzstan
      ".com.kh", ".edu.kh", ".gov.kh", ".org.kh",
      ".kh", // 71      Cambodia
      ".ki", // 105     Kiribati
      ".km", // 2       Comoros
      ".kn", // 4       Saint Kitts and Nevis
      ".kp", // 0       Korea, Democratic People's Republic
      ".ac.kr", ".co.kr", ".go.kr", ".or.kr",
      ".kr", // 27169   Korea, Republic of
      ".com.kw", ".edu.kw", ".gov.kw", ".net.kw", ".org.kw",
      ".kw", // 55      Kuwait
      ".com.ky", ".gov.ky", ".org.ky",
      ".ky", // 81      Cayman Islands
      ".edu.kz", ".gov.kz",
      ".kz", // 703     Kazakhstan
      ".edu.la", ".gov.la",
      ".la", // 213     Lao People's Democratic Republic
      ".com.lb", ".edu.lb", ".gov.lb", ".net.lb", ".org.lb",
      ".lb", // 345     Lebanon
      ".com.lc", ".gov.lc", ".org.lc",
      ".lc", // 15      Saint Lucia
      ".li", // 531     Liechtenstein
      ".com.lk", ".edu.lk", ".gov.lk", ".org.lk",
      ".lk", // 166     Sri Lanka
      ".lr", // 1       Liberia
      ".gov.ls", ".org.ls",
      ".ls", // 12      Lesotho
      ".lt", // 4056    Lithuania
      ".lu", // 1560    Luxembourg
      ".com.lv", ".edu.lv", ".gov.lv", ".net.lv", ".org.lv",
      ".lv", // 3779    Latvia
      ".ly", // 42      Libyan Arab Jamahiriya
      ".ac.ma", ".co.ma", ".gov.ma", ".net.ma", ".org.ma",
      ".ma", // 253     Morocco
      ".mc", // 66      Monaco
      ".com.md", ".net.md", ".org.md",
      ".md", // 643     Moldova, Republic of
      ".gov.mg",
      ".mg", // 41      Madagascar
      ".mh", // 0       Marshall Islands
      ".com.mk", ".edu.mk", ".gov.mk", ".net.mk", ".org.mk",
      ".mk", // 387     Macedonia, The Former Yugoslav Republic of
      ".com.ml", ".gov.ml", ".net.ml", ".org.ml",
      ".ml", // 23      Mali
      ".ac.mn", ".com.mm", ".gov.mm", ".net.mm", ".org.mm",
      ".mm", // 23      Myanmar
      ".edu.mn", ".gov.mn", ".org.mn",
      ".mn", // 179     Mongolia
      ".com.mo", ".edu.mo", ".gov.mo", ".net.mo", ".org.mo",
      ".mo", // 123     Macau
      ".gov.mp", ".org.mp",
      ".mp", // 19      Northern Mariana Islands
      ".mq", // 14      Martinique
      ".mr", // 26      Mauritania
      ".gov.ms", ".net.ms",
      ".ms", // 991     Montserrat
      ".com.mt", ".edu.mt", ".gov.mt", ".net.mt", ".org.mt",
      ".mt", // 265     Malta
      ".ac.mu", ".co.mu", ".gov.mu", ".org.mu",
      ".mu", // 218     Mauritius
      ".com.mv", ".edu.mv", ".gov.mv", ".net.mv", ".org.mv",
      ".mv", // 36      Maldives
      ".ac.mw", ".gov.mw", ".org.mw",
      ".mw", // 19      Malawi
      ".com.mx", ".edu.mx", ".gob.mx", ".net.mx", ".org.mx",
      ".mx", // 3961    Mexico
      ".com.my", ".edu.my", ".gov.my", ".net.my", ".org.my",
      ".my", // 1911    Malaysia
      ".ac.mz", ".co.mz", ".gov.mz", ".org.mz",
      ".mz", // 59      Mozambique
      ".com.na", ".edu.na", ".gov.na", ".org.na",
      ".na", // 237     Namibia
      ".nc", // 73      New Caledonia
      ".ne", // 10      Niger
      ".com.nf", ".edu.nf", ".gov.nf", ".net.nf",
      ".nf", // 23      Norfolk Island
      ".com.ng", ".edu.ng", ".gov.ng", ".net.ng", ".org.ng",
      ".ng", // 29      Nigeria
      ".com.ni", ".edu.ni", ".gob.ni", ".net.ni", ".org.ni",
      ".ni", // 195     Nicaragua
      ".gov.nl",
      ".nl", // 103407  Netherlands
      ".no", // 16284   Norway
      ".com.np", ".edu.np", ".gov.np", ".net.np", ".org.np",
      ".np", // 196     Nepal
      ".nr", // 312     Nauru
      ".gov.nu",
      ".nu", // 8011    Niue
      ".ac.nz", ".co.nz", ".net.nz", ".org.nz",
      ".nz", // 15788   New Zealand
      ".com.om", ".edu.om", ".gov.om", ".net.om", ".org.om",
      ".om", // 41      Oman
      ".com.pa", ".edu.pa", ".gob.pa", ".net.pa", ".org.pa",
      ".pa", // 142     Panama
      ".com.pe", ".edu.pe", ".gob.pe", ".net.pe", ".org.pe",
      ".pe", // 736     Peru
      ".gov.pf",
      ".pf", // 91      French Polynesia
      ".ac.pg", ".com.pg", ".gov.pg", ".net.pg", ".org.pg",
      ".pg", // 71      Papua New Guinea
      ".com.ph", ".edu.ph", ".gov.ph", ".net.ph", ".org.ph",
      ".ph", // 1171    Philippines
      ".com.pk", ".edu.pk", ".gov.pk", ".net.pk", ".org.pk",
      ".pk", // 416     Pakistan
      ".com.pl", ".edu.pl", ".gov.pl", ".net.pl", ".org.pl", ".ac.pl", ".or.pl",
      ".pl", // 72485   Poland
      ".pm", // 1       Saint Pierre and Miquelon
      ".ac.pn", ".gov.pn",
      ".pn", // 44      Pitcairn Island
      ".com.pr", ".edu.pr", ".gov.pr",
      ".pr", // 51      Puerto Rico
      ".gov.ps", ".org.ps",
      ".ps", // 27      Palestinian Territories
      ".com.pt", ".edu.pt", ".gov.pt", ".org.pt",
      ".pt", // 5245    Portugal
      ".pw", // 1       Palau
      ".com.py", ".edu.py", ".gov.py", ".net.py", ".org.py",
      ".py", // 198     Paraguay
      ".com.qa", ".edu.qa", ".gov.qa", ".net.qa", ".org.qa",
      ".qa", // 36      Qatar
      ".re", // 11      Reunion Island
      ".ac.ro", ".co.ro", ".or.ro", ".com.ro", ".edu.ro", ".gov.ro", ".org.ro",
      ".ro", // 8595    Romania
      ".ac.ru", ".co.ru", ".or.ru",
      ".com.ru", ".inc.ru", ".edu.ru", ".gov.ru", ".net.ru", ".org.ru",
      ".ru", // 78631   Russian Federation
      ".ac.rw", ".gov.rw", ".org.rw",
      ".rw", // 19      Rwanda
      ".com.sa", ".edu.sa", ".gov.sa", ".net.sa", ".org.sa",
      ".sa", // 459     Saudi Arabia
      ".com.sb", ".gov.sb", ".net.sb",
      ".sb", // 13      Solomon Islands
      ".com.sc", ".gov.sc",
      ".sc", // 17      Seychelles
      ".gov.sd",
      ".sd", // 1       Sudan
      ".ac.se", ".gov.se", ".org.se",
      ".se", // 17535   Sweden
      ".com.sg", ".edu.sg", ".gov.sg", ".net.sg", ".org.sg",
      ".sg", // 2611    Singapore
      ".edu.sh", ".gov.sh",
      ".sh", // 116     Saint Helena
      ".gov.si",
      ".si", // 2294    Slovenia
      ".sj", // 0       Svalbard and Jan Mayen Islands
      ".edu.sk", ".gov.sk",
      ".sk", // 7312    Slovak Republic
      ".sl", // 0       Sierra Leone
      ".sm", // 136     San Marino
      ".org.sn",
      ".sn", // 83      Senegal
      ".so", // 0       Somalia
      ".sr", // 366     Suriname
      ".st", // 3441    Sao Tome and Principe
      ".su", // 419     Soviet Union
      ".com.sv", ".edu.sv", ".gob.sv", ".org.sv",
      ".sv", // 216     El Salvador
      ".com.sy", ".gov.sy", ".net.sy", ".org.sy",
      ".sy", // 9       Syrian Arab Republic
      ".gov.sz", ".org.sz",
      ".sz", // 23      Swaziland
      ".edu.tc", ".net.tc",
      ".tc", // 1170    Turks and Caicos Islands
      ".td", // 5       Chad
      ".edu.tf", ".net.tf",
      ".tf", // 1031    French Southern Territories
      ".tg", // 15      Togo
      ".ac.th", ".co.th", ".go.th", ".net.th", ".or.th",
      ".th", // 2553    Thailand
      ".tj", // 2       Tajikistan
      ".tk", // 7230    Tokelau
      ".tl", // 0       Timor-Leste
      ".edu.tm", ".gov.tm",
      ".tm", // 37      Turkmenistan
      ".com.tn", ".gov.tn", ".org.tn",
      ".tn", // 157     Tunisia
      ".gov.to",
      ".to", // 4541    Tonga
      ".gov.tp",
      ".tp", // 72      East Timor
      ".com.tr", ".edu.tr", ".gov.tr", ".net.tr", ".org.tr",
      ".tr", // 3912    Turkey
      ".edu.tt", ".gov.tt", ".net.tt", ".org.tt",
      ".tt", // 503     Trinidad and Tobago
      ".co.tv", ".or.tv", ".com.tv", ".org.tv",
      ".tv", // 7781    Tuvalu
      ".com.tw", ".edu.tw", ".gov.tw", ".idv.tw", ".net.tw", ".org.tw",
      ".tw", // 10403   Taiwan
      ".ac.tz", ".co.tz", ".or.tz",
      ".tz", // 68      Tanzania
      ".com.ua", ".edu.ua", ".gov.ua", ".net.ua", ".org.ua",
      ".ua", // 7285    Ukraine
      ".ac.ug", ".co.ug", ".or.ug",
      ".ug", // 83      Uganda
      ".ac.uk", ".co.uk", ".gov.uk", ".ltd.uk", ".me.uk", ".mod.uk", ".nhs.uk",
      ".org.uk", ".plc.uk", ".sch.uk",
      ".uk", // 143510  United Kingdom
      ".um", // 0       United States Minor Outlying Islands
      ".ak.us", ".al.us", ".ar.us", ".az.us", ".ca.us", ".co.us", ".ct.us",
      ".dc.us", ".de.us", ".fl.us", ".ga.us", ".hi.us", ".ia.us", ".id.us",
      ".il.us", ".in.us", ".ks.us", ".ky.us", ".la.us", ".ma.us", ".md.us",
      ".me.us", ".mi.us", ".mn.us", ".mo.us", ".ms.us", ".mt.us", ".nc.us",
      ".nd.us", ".ne.us", ".nh.us", ".nj.us", ".nm.us", ".nv.us", ".ny.us",
      ".oh.us", ".ok.us", ".or.us", ".pa.us", ".ri.us", ".sc.us", ".sd.us",
      ".tn.us", ".tx.us", ".ut.us", ".va.us", ".vt.us", ".wa.us", ".wi.us",
      ".wv.us", ".wy.us",
      ".us", // 32599   United States
      ".com.uy", ".edu.uy", ".gub.uy", ".net.uy", ".org.uy",
      ".uy", // 490     Uruguay
      ".com.uz", ".gov.uz", ".org.uz",
      ".uz", // 202     Uzbekistan
      ".va", // 3       Holy See (Vatican City State)
      ".com.vc",
      ".vc", // 18      Saint Vincent and the Grenadines
      ".com.ve", ".edu.ve", ".gov.ve", ".net.ve", ".org.ve",
      ".ve", // 698     Venezuela
      ".gov.vg", ".net.vg",
      ".vg", // 259     Virgin Islands, British
      ".gov.vi",
      ".vi", // 54      Virgin Islands, U.S.
      ".com.vn", ".edu.vn", ".gov.vn", ".net.vn", ".org.vn",
      ".vn", // 585     Vietnam
      ".com.vu", ".edu.vu", ".gov.vu", ".net.vu", ".org.vu",
      ".vu", // 3742    Vanuatu
      ".wf", // 1       Wallis and Futuna Islands
      ".com.ws", ".edu.ws", ".gov.ws", ".org.ws",
      ".ws", // 9230    Western Samoa
      ".com.ye", ".edu.ye", ".gov.ye", ".org.ye",
      ".ye", // 40      Yemen
      ".yt", // 1       Mayotte
      ".co.yu",
      ".ac.yu", ".edu.yu", ".gov.yu", ".net.yu", ".org.yu",
      ".yu", // 2131    Yugoslavia
      ".ac.za", ".co.za", ".edu.za", ".gov.za", ".org.za",
      ".za", // 8866    South Africa
      ".ac.zm", ".co.zm", ".com.zm", ".edu.zm", ".gov.zm", ".org.zm",
      ".zm", // 62      Zambia
      ".ac.zw", ".co.zw", ".gov.zw", ".org.zw",
      ".zw", // 129     Zimbabwe
    };
  }
}
