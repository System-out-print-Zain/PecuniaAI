## Requirements

Scope:
	- The app is meant for investors in the Canadian Stock Market. We will restrict the scope of the knowledge base to be standard financial documents related to the top 20-30 companies in the S&P/TSX Composite Index. 

**Functional**

- As a user, I want to use natural language to ask about a company's financial state so that I can quickly know relevant data without sifting through documents.
- As a user, I want the information provided by the system to be up to date and have a high rate of accuracy (80+%).
- As a user, I want to create and account so that I can login and save my conversations.
- As a user, I want the AI agent to cite the source documents (e.g., specific 10-K report section, news article URL) for its answers, so that I can verify the information and dig deeper if needed.
- As a user, I want to be able to ask follow-up questions related to a previous answer, so that I can explore topics in more detail within the same conversation context.
- As a user, I want the agent to clearly state if it cannot find relevant information within its knowledge base to answer my query, so that I understand the limitations of its response.
- As a user, I want to be reminded that the system is for informational use only and is not intended as professional financial advice, so that I remember how it should be used.

**Non-Functional**

*Security*: 
- The system shall ensure the privacy of user queries and conversations.
- All sensitive data (e.g., user profiles, API keys) shall be encrypted at rest and in transit.

*Maintainability*:
- The codebase and infrastructure configuration shall be modular, well-documented, and adhere to industry best practices to facilitate future development and debugging.