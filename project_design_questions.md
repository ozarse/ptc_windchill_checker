# Project Design Questions

## API & Authentication

1. What's the base URL format for your Windchill instance? Something like <https://windchill.yourcompany.com/Windchill/servlet/odata/v8/>? Do you know which version of the OData API you're on (v4, PTC's custom variant)?

    - I will not be providing the URL; leave this an an env variable. No idea which odata version. It's a custom API though.

1. How do you authenticate? Basic auth, OAuth2, SSO/SAML with a service account, or something else? Do you already have API credentials or a service account?

    - Basic authentication with username/pass. Use keyring for this.

1. Do you have access to the Windchill OData API documentation for your org, or a Swagger/OpenAPI spec? If so, can you share it or point me to it?

    - I do. I will provide it as a JSON file per set of endpoints (there's like different kinds of CRUD operations for different types of objects)

## Data Model

1. When you say "Config Options PDP", "Part PDP", "IFU Document", "Product Design" -- are these Windchill types (subtypes of wt.doc.WTDocument or wt.part.WTPart), or are they soft type names / display names? Do you know the internal type identifiers?

    - These are internally used human names for these types. I can provide a connection between these names and the technical object names in Windchill.

1. What attributes do you care about? All attributes on each type, or a specific subset (e.g., number, name, state, revision, custom attributes like PartNumber, RegulatoryClass, etc.)?

    - Generally speaking, I want to track all attributes, but I want to be able to track sets of attributes. Basically we have specifications and procedures that certain attributes between certain objects should match. Or maybe the attributes in one type of object determine the specific attributes in another object.

1. When you say "compare attributes across these parts" -- do you mean comparing the same attribute values between related objects (e.g., a Part PDP and its associated IFU Document should have matching regulatory info), or comparing revisions of the same object over time?

    - I think this will need to be specified over time. Maybe there's a way to create different checks that this engine I'm creating will check for me.

## Content / PDFs

1. For the attached PDF content -- do you just need to download the PDF files, or do you also want to extract text from them for comparison?

    - I also want to extract text from them. I'm thinking docling because it's open source and because there's a lot of capability of expanding modules and such. As of now, things have to run locally and can't be exported to the world (yet)

1. Are the PDFs primary content on the document, or are they secondary content / attachments?

## Scope & Environment

1. Roughly how many objects are we talking about? Dozens, hundreds, thousands?

    - Hundreds of objects, or maybe less than 5000.

1. Are you running this from a machine that has network access to the Windchill server, or do you need to go through a VPN/proxy?

    - This goes through a VPN. Likely slow.

1. What Python version do you have available? Any constraints on installing packages (e.g., corporate proxy, no pip access)?

## Additional questions and answers

1. Do you want to store this information in a local database?

    - Absolutely. I want to keep this information locally. So maybe there's a function to refresh the information by pulling it from OnePLM through requests, if the last modified date is later than the locally stored version.

1. How do you want to store this information?

    - I don't know. I have limited install access. Everything needs to be done through PIP and python.
