const crypto = window.crypto;
const chatbotToggler = document.querySelector(".chatbot-toggler");
const closeBtn = document.querySelector(".close-btn");
const chatbox = document.querySelector(".chatbox");
const chatInput = document.querySelector(".chat-input textarea");
const sendChatBtn = document.querySelector(".chat-input span");
const inputInitHeight = chatInput.scrollHeight;
const sessionId = Date.now().toString(); // Use current time in milliseconds as session ID
let userMessage = null; // Variable to store user's message

const createChatLi = (message, className) => {
    // Create a chat <li> element with passed message and className
    const chatLi = document.createElement("li");
    chatLi.classList.add("chat", `${className}`);
    let chatContent = className === "outgoing" ? `<p></p>` : `<span class="material-symbols-outlined">smart_toy</span><p></p>`;
    chatLi.innerHTML = chatContent;
    chatLi.querySelector("p").textContent = message;
    return chatLi; // return chat <li> element
}

const generateResponse = async (chatElement) => {
    const messageElement = chatElement.querySelector("p");

    // Define the message payload, formatted for FastAPI
    const payload = JSON.stringify({
        question: userMessage,
        thread_id: sessionId
    });

    // Create "SHA-256" has for the payload
    const payloadBuffer = new TextEncoder().encode(payload);
    const hashBuffer = await crypto.subtle.digest('SHA-256', payloadBuffer);
    const hasArray = Array.from(new Uint8Array(hashBuffer));
    const sha256Hash = hasArray.map(b => b.toString(16).padStart(2, '0')).join('');

    // Define the properties and message for the API request
    const requestOptions = {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            'x-amz-content-sha256': sha256Hash,
        },
        body: payload,
    }

    // Send POST request to API to get the stream response,
    // and set the response as paragraph text
    try {
        const response = await fetch("/api/chat", requestOptions);
        const reader = response.body.getReader();
        messageElement.textContent = "";
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            const text = new TextDecoder().decode(value);
            messageElement.textContent += text;
            chatbox.scrollTo(0, chatbox.scrollHeight);
        }
    } catch (error) {
        messageElement.classList.add("error");
        messageElement.messageElement.textContent = "Something went wrong. Please try again.";
    } finally {
        chatbox.scrollTo(0, chatbox.scrollHeight);
    };
}

const handleChat = () => {
    userMessage = chatInput.value.trim(); // Get user entered message and remove extra whitespace
    if(!userMessage) return;

    // Clear the input textarea and set its height to default
    chatInput.value = "";
    chatInput.style.height = `${inputInitHeight}px`;

    // Append the user's message to the chatbot
    chatbox.appendChild(createChatLi(userMessage, "outgoing"));
    chatbox.scrollTo(0, chatbox.scrollHeight);
    
    setTimeout(() => {
        // Display "Thinking..." message while waiting for the response
        const incomingChatLi = createChatLi("Thinking...", "incoming");
        chatbox.appendChild(incomingChatLi);
        chatbox.scrollTo(0, chatbox.scrollHeight);
        generateResponse(incomingChatLi);
    }, 600);
}

chatInput.addEventListener("input", () => {
    // Adjust the height of the input textarea based on its content
    chatInput.style.height = `${inputInitHeight}px`;
    chatInput.style.height = `${chatInput.scrollHeight}px`;
});

chatInput.addEventListener("keydown", (e) => {
    // If Enter key is pressed without Shift key and the window 
    // width is greater than 800px, handle the chat
    if(e.key === "Enter" && !e.shiftKey && window.innerWidth > 800) {
        e.preventDefault();
        handleChat();
    }
});

sendChatBtn.addEventListener("click", handleChat);
closeBtn.addEventListener("click", () => document.body.classList.remove("show-chatbot"));
chatbotToggler.addEventListener("click", () => document.body.classList.toggle("show-chatbot"));