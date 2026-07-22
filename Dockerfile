FROM python:3.10-slim

# হাগিং ফেস-এর সিকিউরিটি এবং ফাইল রাইট পারমিশনের জন্য ইউজার সেটআপ
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

# লাইব্রেরি ইনস্টল করা
COPY --chown=user requirements.txt $HOME/app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# সব ফাইল কপি করা
COPY --chown=user . $HOME/app

# হাগিং ফেস-এর ডিফল্ট পোর্ট ৭৮৬০-তে সার্ভার রান করা
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]