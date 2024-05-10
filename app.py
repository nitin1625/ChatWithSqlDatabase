from dotenv import load_dotenv
import streamlit as st
from langchain_community.chat_models.openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import  AIMessage,HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_groq import ChatGroq
import urllib.parse
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
def init_database(user_info,password_info,SERVER_NAME,DATABASE_NAME):

    DRIVER_NAME = 'ODBC Driver 17 for SQL Server'
    connection_str = f'DRIVER={DRIVER_NAME};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={user_info};PWD={password_info};TrustServerCertificate=yes'
    conn = pyodbc.connect(connection_str)
    # Quote the connection string
    quoted = urllib.parse.quote_plus(connection_str)
    target_connection = f'mssql+pyodbc:///?odbc_connect={quoted}'
    engine = create_engine(target_connection)
    db=SQLDatabase(engine)
    return db


def get_sql_chain(db):
    template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, write a SQL query that would answer the user's question. Take the conversation history into account.

    <SCHEMA>{schema}</SCHEMA>

    Conversation History: {chat_history}

    Write only the SQL query and nothing else. Do not wrap the SQL query in any other text, not even backticks.

    For example:
    Question: which 3 artists have the most tracks?
    SQL Query: SELECT TOP 3 ArtistId, COUNT(*) as track_count FROM Track GROUP BY ArtistId ORDER BY track_count DESC;

    Question: Name 10 artists
    SQL Query: SELECT TOP 10 Name FROM Artist;


    Your turn:

    Question: {question}
    SQL Query:
    """

    prompt = ChatPromptTemplate.from_template(template)
    llm = ChatOpenAI(model="gpt-3.5-turbo")
    # llm = ChatGroq(model="mixtral-8x7b-32768", temperature=0)

    def get_schema(_):
        return db.get_table_info()

    return (
            RunnablePassthrough.assign(schema=get_schema)
            | prompt
            | llm
            | StrOutputParser()
    )


def get_response(user_query: str, db: SQLDatabase, chat_history: list):
    sql_chain = get_sql_chain(db)

    template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, question, sql query, and sql response, write a natural language response.
    <SCHEMA>{schema}</SCHEMA>

    Conversation History: {chat_history}
    SQL Query: <SQL>{query}</SQL>
    User question: {question}
    SQL Response: {response}"""

    prompt = ChatPromptTemplate.from_template(template)

    llm = ChatOpenAI(model="gpt-3.5-turbo")
    # llm = ChatGroq(model="mixtral-8x7b-32768", temperature=0)

    chain = (
            RunnablePassthrough.assign(query=sql_chain).assign(
                schema=lambda _: db.get_table_info(),
                response=lambda vars: db.run(vars["query"]),
            )
            | prompt
            | llm
            | StrOutputParser()
    )

    return chain.invoke({
        "question": user_query,
        "chat_history": chat_history,
    })



if "chat_history" not in st.session_state:
    st.session_state.chat_history=[
        AIMessage(content="Hello! I'm SQL assistant. Ask me Anything about your database."),]
load_dotenv()

st.set_page_config(page_title="Chat With Database",page_icon=":speech_balloon:")
st.title("Database Automation")
#
with st.sidebar:
    st.subheader("Settings")
    st.write("Connect to database and start chatting.")

    # st.text_input("Server/Host",value="DT-SN001\SQLEXPRESS",key="Host")
    # st.text_input("User", value="sa",key="User")
    # st.text_input("Password",type="password", value="Sonar@2152",key="Password")
    # st.text_input("Database", value="equity",key="Database")

    st.text_input("Server/Host", value="alpha\SQLEXPRESS", key="Host")
    st.text_input("User", value="uid", key="User")
    st.text_input("Password", type="password", value="nitin@2152", key="Password")
    st.text_input("Database", value="database", key="Database")

    if st.button("Connect"):
        with st.spinner("Connecting to database..."):
            db=init_database(
                st.session_state["User"],
                st.session_state["Password"],
                st.session_state["Host"],
                # st.session_state["Port"],
                st.session_state["Database"]
            )

            st.session_state.db=db
            st.success("Connected to database!")

for message in st.session_state.chat_history:
    if isinstance(message,AIMessage):
        with st.chat_message("AI"):
            st.markdown(message.content)

    elif isinstance(message,HumanMessage):
        with st.chat_message("Human"):
            st.markdown(message.content)






user_query=st.chat_input("Type a message...")

if user_query is not None and user_query.strip()!="":
    st.session_state.chat_history.append(HumanMessage(content=user_query))

    with st.chat_message("Human"):
        st.markdown(user_query)

    with st.chat_message("AI"):
        try:
            response=get_response(user_query,st.session_state.db,st.session_state.chat_history)
        except Exception as e:
            response="I am Still Learning !!!"
        st.markdown(response)

    st.session_state.chat_history.append(AIMessage(content=response))